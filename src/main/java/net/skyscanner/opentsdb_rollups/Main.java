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

import net.skyscanner.opentsdb_rollups.config.ArgParser;
import net.skyscanner.opentsdb_rollups.config.Config;
import net.skyscanner.opentsdb_rollups.config.ConfigParser;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        Main.run(args);
    }

    private static void run(String[] args) {

        try {
            ArgParser argParser = new ArgParser(args);
            Config cfg = ConfigParser.parse(argParser.getConfigName());
            SparkConf sparkConf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
            JavaSparkContext javaSparkContext = new JavaSparkContext(argParser.getSparkMaster(), "RollupJob", sparkConf);
            new RollupJob(javaSparkContext, cfg).run(cfg.getKafkaConfig(), cfg.getTsdbConfig());
        } catch (Exception e) {
            log.info(e.toString());
            for (StackTraceElement ste : e.getStackTrace()) {
                log.info(ste.toString());
            }

            System.exit(1);
        } finally {
            log.info("Exiting");
        }
    }
}
