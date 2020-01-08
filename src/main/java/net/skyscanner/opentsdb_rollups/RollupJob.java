/**
 * Copyright 2020 Skyscanner Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.skyscanner.opentsdb_rollups;

import net.opentsdb.core.HistogramDataPoint;
import net.opentsdb.core.Internal;
import net.skyscanner.opentsdb_rollups.config.Config;
import net.skyscanner.opentsdb_rollups.config.HBaseConfig;
import net.skyscanner.opentsdb_rollups.config.KafkaConfig;
import net.skyscanner.opentsdb_rollups.config.TimeFilterConfig;
import net.skyscanner.opentsdb_rollups.config.TsdbConfig;
import net.skyscanner.opentsdb_rollups.filter.TimeFilter;
import net.skyscanner.opentsdb_rollups.parser.TsdbRowKey;
import net.skyscanner.opentsdb_rollups.parser.TsdbTableParser;
import net.skyscanner.opentsdb_rollups.producer.RollupKafkaProducer;
import net.skyscanner.opentsdb_rollups.resolver.ResolvedTSUID;
import net.skyscanner.opentsdb_rollups.resolver.UidResolver;
import net.skyscanner.opentsdb_rollups.serializer.RollupMessageSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.hbase.async.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


class RollupJob {
    private static final Logger log = LoggerFactory.getLogger(RollupJob.class);
    private static final byte[] T_TABLE_FAMILY = Bytes.toBytes("t");
    private final JavaSparkContext sparkContext;
    private final TimeFilterConfig timeFilterConfig;
    private final HBaseConfig hbaseConfig;

    RollupJob(JavaSparkContext sparkContext, Config cfg) {
        this.sparkContext = sparkContext;
        this.hbaseConfig = cfg.getHbaseConfig();
        this.timeFilterConfig = cfg.getTimeFilterConfig();
    }

    private static Configuration newHBaseConfig(String path) {
        Configuration hbaseConf = new Configuration();
        hbaseConf.addResource(path);
        return hbaseConf;
    }

    private static String convertScanToString(Scan scan) throws IOException {
        ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
        return Base64.getEncoder().encodeToString(proto.toByteArray());
    }

    private JavaPairRDD<ImmutableBytesWritable, Result> newTsdbRDD(String tableName) throws IOException {
        Scan scan = new Scan();

        /**
         * The time range filter looks at the column time stamp,
         * but don't want partial rows so add a grace period on the end to capture some of the next hour.
         * Use an additional row filter to filter out the partial row which is outwith the time range.
         */
        if (timeFilterConfig.isUseHbaseServerTimeFilter()) {
            scan.setTimeRange(
                    timeFilterConfig.getStartTime() - timeFilterConfig.getGracePeriodMs(),
                    timeFilterConfig.getEndTime() + timeFilterConfig.getGracePeriodMs()
            );
            log.info("Set HBaseServerTimeFilter: {}", scan.getTimeRange());
        } else {
            log.info("Not using hbase insertion time filters");
        }

        if (timeFilterConfig.isUseMetricTimeRangeFilter()) {
            scan.setFilter(TimeFilter.StartAndEnd(
                    Math.toIntExact(timeFilterConfig.getStartTime() / 1000l),
                    Math.toIntExact(timeFilterConfig.getEndTime() / 1000l)
            ));
            log.info("Using row time range filter: start={} end={}", timeFilterConfig.getStartTime(), timeFilterConfig.getEndTime());
        } else {
            log.info("Not using row time range filter");
        }

        List<Integer> filterTimes = timeFilterConfig.getTimestampsToFind();
        if (filterTimes != null && !filterTimes.isEmpty()) {
            scan.setFilter(TimeFilter.List(filterTimes));
            log.info("Using time list filters: {}", filterTimes);
        }

        scan.setScanMetricsEnabled(false);

        Configuration hBaseConfig = newHBaseConfig(this.hbaseConfig.getSiteXmlPath());

        hBaseConfig.set(TableInputFormat.SCAN, convertScanToString(scan));
        hBaseConfig.set(TableInputFormat.INPUT_TABLE, tableName);
        hBaseConfig.set("hbase.TableSnapshotInputFormat.snapshot.name", this.hbaseConfig.getTsdbTableSnapshotName());
        hBaseConfig.set("hbase.TableSnapshotInputFormat.restore.dir", this.hbaseConfig.getHbaseTableSnapshotInputFormatRestoreDir());
        hBaseConfig.set("hbase.rootdir", this.hbaseConfig.getHbaseRootdir());

        log.info("HBase client config: {}", hBaseConfig);

        return this.sparkContext.newAPIHadoopRDD(hBaseConfig, TableSnapshotInputFormat.class, ImmutableBytesWritable.class, Result.class);
    }

    void run(KafkaConfig kafkaConfig, TsdbConfig tsdbConfig) throws IOException {
        JavaPairRDD<ImmutableBytesWritable, Result> tsdb = this.newTsdbRDD("tsdb");
        Broadcast<UidResolver> uidResolver = this.sparkContext.broadcast(new UidResolver(tsdbConfig.getZookeeperNodes()));

        tsdb.mapValues(RollupJob::aggregateRow).foreachPartition(p -> {
            RollupKafkaProducer kafkaProducer = new RollupKafkaProducer(kafkaConfig);
            kafkaProducer.startClock();
            UidResolver resolver = uidResolver.value();
            resolver.initTsdb();

            p.forEachRemaining(rowkeyToAggregate -> produceRollupMessage(kafkaProducer, resolver, rowkeyToAggregate));
            kafkaProducer.await();
        });
    }

    /**
     * Produces a {@link net.skyscanner.schemas.OpenTSDB.RollupMessage} to Kafka
     *
     * Takes a row key and the {@link Aggregate} produced by the previous stage, resolves the UIDs and combines them
     * into a rollup message that's being sent to Kafka for ingestion
     *
     * @param kafkaProducer     The Kafka producer to use
     * @param resolver          The UID resolver to use
     * @param rowkeyToAggregate The Tuple of row (key, aggregate) generated by the previous stage
     */
    private static void produceRollupMessage(RollupKafkaProducer kafkaProducer, UidResolver resolver, Tuple2<ImmutableBytesWritable, Aggregate> rowkeyToAggregate) {
        if (rowkeyToAggregate == null) return;

        try {
            TsdbRowKey rowKey = TsdbTableParser.parseTsdbTableRow(rowkeyToAggregate._1().get());

            Aggregate agg = rowkeyToAggregate._2();
            if (agg == null) {
                return;
            }
            ResolvedTSUID resolvedTSUID = resolver.getBatch(rowKey.getMetricUid(), rowKey.getTags());

            kafkaProducer.sendMessage(
                    rowKey.getMetricUid(),
                    RollupMessageSerializer.createRollupMessage(
                            resolvedTSUID.getMetric(),
                            resolvedTSUID.getTags(),
                            rowKey.getTimestamp(),
                            agg.getSum(),
                            agg.getMin(),
                            agg.getMax(),
                            agg.getCount()
                    )
            );
        } catch (Exception e) {
            log.error("Exception when trying to add a message to the send queue", e);
        }
    }

    /**
     * Aggregates the values of a row
     *
     * @param row The {@link Result} containing the individual HBase cells of the row as returned by the {@link CustomTableSnapshotInputFormat}
     * @return An {@link Aggregate} with the sum, count, min and max value of the values from this row
     */
    private static Aggregate aggregateRow(Result row) {
        List<Double> dataPoints = new ArrayList<>();

        Map<byte[], byte[]> qualifierToValue = row.getFamilyMap(T_TABLE_FAMILY);
        qualifierToValue.forEach((qualifier, valueBytes) -> {
            if (qualifier[0] == HistogramDataPoint.PREFIX) {
                // TODO: Do we want to roll up histograms? Can we?
                return;
            }

            dataPoints.addAll(extractPointsFromQualifier(row.getRow(), qualifier, valueBytes));
        });

        if (dataPoints.isEmpty()) {
            return null;
        }

        DoubleSummaryStatistics stats = dataPoints.stream().collect(Collectors.summarizingDouble(Double::doubleValue));

        return new Aggregate(stats.getSum(), stats.getMin(), stats.getMax(), stats.getCount());
    }

    /**
     * Extracts points from an HBase cell represented by a qualifier/value pair
     * <p>
     * The qualifier/value pair can contain multiple data points if we're looking at a compacted cell; in which case
     * the qualifiers of previously separate HBase cells have been compacted by OpenTSDB, meaning the bytes of both the
     * qualifiers and values of the previous HBase cells have been concatenated
     *
     * The name of the {@link Internal.Cell} class used by OpenTSDB is misleading and doesn't have anything to do with
     * HBase cells. It's just a wrapper around a qualifier and a value, presumably because they once made the assumption
     * that every HBase cell could only hold one value.
     *
     * @param row        The byte array representing the row key
     * @param qualifier  The column qualifier
     * @param valueBytes The byte array holding the values to decode
     * @return A list of extracted values from this HBase cell
     */
    private static List<Double> extractPointsFromQualifier(byte[] row, byte[] qualifier, byte[] valueBytes) {
        try {
            KeyValue kv = new KeyValue(row, T_TABLE_FAMILY, qualifier, valueBytes);
            List<Internal.Cell> extractedDataPoints = Internal.extractDataPoints(kv);

            return extractedDataPoints.stream()
                    .map(Internal.Cell::parseValue)
                    .map(Number::doubleValue)
                    .collect(Collectors.toList()
                    );

        } catch (Exception e) {
            log.error("Exception thrown while parsing values for qualifiers", e);
            return new ArrayList<>();
        }
    }
}
