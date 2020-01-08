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

import io.vavr.control.Try;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

public class ArgParser {

    private static final String SPARK_MASTER = "spark-master";
    private static final String CONFIGURATION = "config";

    private final String sparkMaster;
    private final String configName;

    public ArgParser(String[] args) {
        OptionSet options = Try.of(() -> {
            OptionParser parser = new OptionParser();
            parser.accepts(SPARK_MASTER).withRequiredArg().required();
            parser.accepts(CONFIGURATION).withRequiredArg().required();

            return parser.parse(args);
        }).getOrElseThrow(e -> new RuntimeException("Invalid arguments: " + e.getMessage(), e));

        sparkMaster = options.valueOf(SPARK_MASTER).toString().trim();
        configName = options.valueOf(CONFIGURATION).toString().trim();
    }

    public String getSparkMaster() {
        return sparkMaster;
    }

    public String getConfigName() {
        return configName;
    }
}
