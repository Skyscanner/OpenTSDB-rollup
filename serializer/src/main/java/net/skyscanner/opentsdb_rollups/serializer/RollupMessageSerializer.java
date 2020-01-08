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

package net.skyscanner.opentsdb_rollups.serializer;

import net.skyscanner.schemas.OpenTSDB;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class RollupMessageSerializer implements Serializer<OpenTSDB.RollupMessage> {

    public static OpenTSDB.RollupMessage createRollupMessage(String metricName,
                                                             Map<String, String> tags,
                                                             long unixTimeMillis,
                                                             double sum, double min, double max, long count) {
        return OpenTSDB.RollupMessage.newBuilder()
                .setHeader(
                        OpenTSDB.LightHeader.newBuilder().setEventTimestamp(
                                OpenTSDB.DateTime.newBuilder().setUnixTimeMillis(unixTimeMillis)
                        )
                )
                .setMetric(metricName)
                .putAllTags(tags)
                .setSum(sum)
                .setMin(min)
                .setMax(max)
                .setCount(count)
                .build();
    }

    @Override
    public byte[] serialize(String topic, OpenTSDB.RollupMessage data) {
        return data == null ? null : data.toByteArray();
    }

    @Override
    public byte[] serialize(String topic, Headers headers, OpenTSDB.RollupMessage data) {
        return serialize(topic, data);
    }
}
