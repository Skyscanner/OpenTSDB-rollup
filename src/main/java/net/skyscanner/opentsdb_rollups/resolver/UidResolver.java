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

package net.skyscanner.opentsdb_rollups.resolver;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import net.opentsdb.core.TSDB;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;
import org.hbase.async.HBaseClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

public class UidResolver implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(UidResolver.class);

    private static final String ZK_BASEDIR = "tsd.storage.hbase.zk_basedir";
    private static final String ZK_QUORUM = "tsd.storage.hbase.zk_quorum";
    private static final String TSDB_MODE = "tsdb.mode";
    private static final String TAGV_LENGTH = "tsd.storage.uid.width.tagv";
    private static final String NAME_CACHE_SIZE = "tsd.uid.lru.name.size";
    private static final String ID_CACHE_SIZE = "tsd.uid.lru.id.size";
    private static final String CACHE_PATH = "tsd.uid.disk_cache.directory";

    private static final String tagvLength = "4";
    private final String tsdbMode = "ro";
    private final String zkDirectory = "/hbase";
    private final String zkNodes;

    private transient TSDB tsdb;

    public UidResolver(List<String> zookeeperNodes) {
        this.zkNodes = String.join(",", zookeeperNodes);
    }

    public void initTsdb() throws IOException {
        if (this.tsdb == null) {
            log.info("Initialising TSDB...");
            Config config = new Config(false);
            config.overrideConfig(ZK_BASEDIR, zkDirectory);
            config.overrideConfig(ZK_QUORUM, zkNodes);
            config.overrideConfig(TSDB_MODE, tsdbMode);
            config.overrideConfig(TAGV_LENGTH, tagvLength);
            config.overrideConfig(NAME_CACHE_SIZE, "10000000");
            config.overrideConfig(ID_CACHE_SIZE, "10000000");
            config.overrideConfig(CACHE_PATH, ".");
            org.hbase.async.Config hbaseConfig = new org.hbase.async.Config();
            hbaseConfig.overrideConfig("hbase.workers.size", "32");
            hbaseConfig.overrideConfig("hbase.zookeeper.znode.parent", config.getString(ZK_BASEDIR));
            hbaseConfig.overrideConfig("hbase.zookeeper.quorum", config.getString(ZK_QUORUM));

            this.tsdb = new TSDB(new HBaseClient(hbaseConfig), config);
            log.info("TSDB initialised.");
        }
    }

    public ResolvedTSUID getBatch(byte[] metricUid, Map<byte[], byte[]> tagUids) throws Exception {
        initTsdb();
        Map<Deferred<String>, Deferred<String>> resolvedTags = new HashMap<>();
        Deferred<String> resolvedMetric = getMetricName(metricUid);
        for (Map.Entry<byte[], byte[]> entry : tagUids.entrySet()) {
            resolvedTags.put(
                    getTagKeyName(entry.getKey()),
                    getTagValueName(entry.getValue())
            );
        }
        Map<String, String> tags = new HashMap<>();
        for (Map.Entry<Deferred<String>, Deferred<String>> entry : resolvedTags.entrySet()) {
            tags.put(entry.getKey().join(), entry.getValue().join());
        }
        return new ResolvedTSUID(resolvedMetric.join(), tags);
    }

    private Deferred<String> getName(UniqueId.UniqueIdType type, final byte[] uid) throws Exception {
        return this.tsdb.getUidName(type, uid)
                .addCallback(arg -> arg)
                .addErrback((Callback<String, Exception>) arg -> {
                    log.error(arg.toString());
                    throw arg;
                });
    }

    private Deferred<String> getMetricName(final byte[] uid) throws Exception {
        return getName(UniqueId.UniqueIdType.METRIC, uid);
    }

    private Deferred<String> getTagKeyName(final byte[] uid) throws Exception {
        return getName(UniqueId.UniqueIdType.TAGK, uid);
    }

    private Deferred<String> getTagValueName(final byte[] uid) throws Exception {
        return getName(UniqueId.UniqueIdType.TAGV, uid);
    }
}
