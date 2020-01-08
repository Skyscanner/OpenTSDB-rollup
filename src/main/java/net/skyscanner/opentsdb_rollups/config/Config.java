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

package net.skyscanner.opentsdb_rollups.config;

public class Config {
    private KafkaConfig kafkaConfig;
    private TsdbConfig tsdbConfig;
    private TimeFilterConfig timeFilterConfig;
    private HBaseConfig hbaseConfig;

    public TimeFilterConfig getTimeFilterConfig() {
        return timeFilterConfig;
    }

    public void setTimeFilterConfig(TimeFilterConfig timeFilterConfig) {
        this.timeFilterConfig = timeFilterConfig;
    }

    public KafkaConfig getKafkaConfig() {
        return kafkaConfig;
    }

    public void setKafkaConfig(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
    }

    public TsdbConfig getTsdbConfig() { return tsdbConfig; }
    public void setTsdbConfig(TsdbConfig tsdbConfig) { this.tsdbConfig = tsdbConfig; }

    public HBaseConfig getHbaseConfig() {
        return hbaseConfig;
    }

    public void setHbaseConfig(HBaseConfig hbaseConfig) {
        this.hbaseConfig = hbaseConfig;
    }
}
