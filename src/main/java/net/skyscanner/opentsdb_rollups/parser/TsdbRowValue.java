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

package net.skyscanner.opentsdb_rollups.parser;

import net.opentsdb.core.HistogramDataPoint;
import net.opentsdb.core.Internal;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.hbase.async.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class TsdbRowValue {

    private static final Logger log = LoggerFactory.getLogger(TsdbRowValue.class);

    private static final byte[] T_TABLE_FAMILY = Bytes.toBytes("t");

    private TsdbRowValue() {}

    /**
     * Aggregates the values of a row
     *
     * @param row The {@link Result} containing the individual HBase cells of the row as returned by the {@link CustomTableSnapshotInputFormat}
     * @return An {@link Aggregate} with the sum, count, min and max value of the values from this row
     */
    public static Aggregate aggregateRow(Result row) {
        List<Double> dataPoints = new ArrayList<>();

        // Get map from qualifier to value, which could contain duplicate timestamps
        Map<byte[], byte[]> qualifierToValue = row.getFamilyMap(T_TABLE_FAMILY);

        /**
         * Dedup based on row offset/timestamp
         *
         * The column qualifier encodes the offset from the base timestamp in the row key as well as the data type
         * stored in the corresponding value.  If a data point is rewritten and its type changes, e.g. from a int32 to
         * int64, the qualifier will be different too.  So the datapoints for the same timestamp will appear twice.
         * The deduping relies on getFamilyMap() returning data in order such that the latest write wins.
         */
        Map<Integer, Map.Entry<byte[], byte[]>> offsetToValue = new TreeMap<>();
        qualifierToValue.entrySet().forEach(entry -> {
            int offset = Internal.getOffsetFromQualifier(entry.getKey());
            offsetToValue.put(offset, entry);
        });

        offsetToValue.forEach((offset, entry) -> {
            byte[] qualifier = entry.getKey();
            byte[] valueBytes = entry.getValue();
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

            Collection<Internal.Cell> extractedDataPoints = Internal.extractDataPoints(kv);
            return extractedDataPoints.stream()
                    .map(Internal.Cell::parseValue)
                    .map(Number::doubleValue)
                    .collect(Collectors.toList());

        } catch (Exception e) {
            log.error("Exception thrown while parsing values for qualifiers", e);
            return new ArrayList<>();
        }
    }
}
