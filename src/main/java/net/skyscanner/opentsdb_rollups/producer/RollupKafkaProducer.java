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

package net.skyscanner.opentsdb_rollups.producer;

import net.skyscanner.opentsdb_rollups.config.KafkaConfig;
import net.skyscanner.opentsdb_rollups.serializer.RollupMessageSerializer;
import net.skyscanner.schemas.OpenTSDB;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Properties;
import java.util.concurrent.*;

public class RollupKafkaProducer {
    private static final Logger log = LoggerFactory.getLogger(RollupKafkaProducer.class);

    private final Producer<byte[], OpenTSDB.RollupMessage> producer;
    private final String topic;
    private final int delayMs;
    private final int periodMs;
    private final int msgPerPeriod;
    private final BlockingQueue<Tuple2<byte[], OpenTSDB.RollupMessage>> queue;
    private TaskClock clock;
    private SendBatch sendBatch = new SendBatch();

    public RollupKafkaProducer(KafkaConfig config) {
        this(config, createProducer(config));
    }

    public RollupKafkaProducer(KafkaConfig config, Producer<byte[], OpenTSDB.RollupMessage> producer) {
        this(config, producer, new LinkedBlockingQueue<>(config.getQueueCapacity()));
    }

    public RollupKafkaProducer(KafkaConfig config,
                               Producer<byte[], OpenTSDB.RollupMessage> producer,
                               BlockingQueue<Tuple2<byte[], OpenTSDB.RollupMessage>> queue) {
        this.producer = producer;
        this.topic = config.getTopic();
        this.msgPerPeriod = config.getMsgPerPeriod();
        this.queue = queue;
        this.delayMs = config.getDelayMs();
        this.periodMs = config.getPeriodMs();
    }

    private static Producer<byte[], OpenTSDB.RollupMessage> createProducer(KafkaConfig config) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getServer());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, config.getTopic());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, RollupMessageSerializer.class.getName());
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, CompressionType.GZIP.name);
        props.put(ProducerConfig.LINGER_MS_CONFIG, "10");

        return new KafkaProducer<>(props);
    }

    public void startClock() {
        clock = new ProducerClock(delayMs, periodMs);
        startClock(clock);
    }

    public void startClock(TaskClock clock) {
        this.clock = clock;
        clock.setListener(sendBatch);
        clock.start();
    }

    public void await() {
        if (clock != null) {
            clock.await();
        }
    }

    public void sendMessage(byte[] key, OpenTSDB.RollupMessage value) {
        try {
            this.queue.put(new Tuple2<>(key, value));
        } catch (InterruptedException e) {
            log.error("Could not put message in queue. Skipping message.", e);
        }
    }

    private class ProducerClock extends TaskClock {
        private final int delayMs;
        private final int periodMs;
        private ScheduledExecutorService executor;

        ProducerClock(int delayMs, int periodMs) {
            this.delayMs = delayMs;
            this.periodMs = periodMs;
            executor = Executors.newScheduledThreadPool(1);
        }

        public void start() {
            executor.scheduleAtFixedRate(super.listener, delayMs, periodMs, TimeUnit.MILLISECONDS);
        }

        public void stop() {
            executor.shutdown();
        }

        public void await() {
            try {
                sendBatch.shutdown();
                executor.awaitTermination(10, TimeUnit.MINUTES);
                producer.close();
            } catch (InterruptedException e) {
                log.error("RollupKafkaProducer interrupted", e);
            }
        }
    }

    private class SendBatch implements Runnable {
        private boolean shuttingDown = false;

        @Override
        public void run() {
            if (shuttingDown && queue.isEmpty()) {
                clock.stop();
                return;
            }
            int batchSize = Math.min(msgPerPeriod, queue.size());
            for (int i = 0; i < batchSize; i++) {
                Tuple2<byte[], OpenTSDB.RollupMessage> msg;
                try {
                    msg = queue.take();
                    producer.send(new ProducerRecord<>(topic, msg._1(), msg._2()));
                } catch (InterruptedException e) {
                    log.error("Could not remove message from queue: {}", e.getMessage());
                }
            }
        }

        /**
         * Call during shutdown sequence to drain the queue
         */
        public void shutdown() {
            shuttingDown = true;
        }
    }
}
