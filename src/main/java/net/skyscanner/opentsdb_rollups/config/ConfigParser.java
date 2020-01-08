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

import net.skyscanner.opentsdb_rollups.producer.RollupKafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.CustomClassLoaderConstructor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class ConfigParser {

    private static final Logger log = LoggerFactory.getLogger(RollupKafkaProducer.class);

    public static Config parse(String configName) {
        try {
            Yaml yaml = new Yaml(new CustomClassLoaderConstructor(Config.class.getClassLoader()));
            return yaml.loadAs(getConfigBytes(configName), Config.class);
        } catch (Exception e) {
            log.error("Error loading config", e);
            return new Config();
        }
    }

    private static ByteArrayInputStream getConfigBytes(String configName) throws IOException {
        return new ByteArrayInputStream(Files.readAllBytes(Paths.get(configName)));
    }
}
