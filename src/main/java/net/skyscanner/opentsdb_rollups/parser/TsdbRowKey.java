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

import org.apache.hadoop.hbase.util.Bytes;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.apache.commons.codec.binary.Hex.encodeHexString;

public class TsdbRowKey {

    private final byte[] metricUid;
    private final int timestamp;

    // Map<tagK, tagV>
    private final Map<byte[], byte[]> tags;

    public TsdbRowKey() {
        this(new byte[0], 0, new HashMap<>());
    }

    public TsdbRowKey(byte[] metricUid, int timestamp, Map<byte[], byte[]> tags) {
        this.metricUid = metricUid;
        this.timestamp = timestamp;
        this.tags = tags;
    }

    public byte[] getMetricUid() {
        return metricUid;
    }

    public String getMetricUidString() {
        return Bytes.toString(metricUid);
    }

    public int getTimestamp() {
        return timestamp;
    }

    public Date getTimestampDate() {
        return new Date((long) timestamp * 1000); // *1000 for milliseconds
    }

    public Map<byte[], byte[]> getTags() {
        return tags;
    }

    public String tsuid() {
        StringBuilder sb = new StringBuilder(encodeHexString(metricUid));
        tags.forEach( (k, v) -> sb.append(encodeHexString(k)).append(encodeHexString(v)));
        return sb.toString();
    }

    public String toString() {
        StringBuilder sb = new StringBuilder(getTimestampDate().toString());
        sb.append(" ").append(getTimestamp());
        sb.append(" ").append(tsuid());
        return sb.toString();
    }
}
