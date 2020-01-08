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

import net.skyscanner.opentsdb_rollups.config.KafkaConfig;
import net.skyscanner.opentsdb_rollups.producer.RollupKafkaProducer;
import net.skyscanner.opentsdb_rollups.producer.TaskClock;
import net.skyscanner.schemas.OpenTSDB;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.junit.MockitoRule;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.fail;

@RunWith(MockitoJUnitRunner.class)
public class RollupKafkaProducerTest {

    private static final int QUEUE_CAPACITY = 5;
    private static final int DELAY_MS = 2000;
    private static final int PERIOD_MS = 1000;
    private static final int MESSAGES_PER_PERIOD = 2;

    private RollupKafkaProducer mockRkp;
    private RollupKafkaProducer realRkp;
    private BlockingQueue mockQueue;
    private ManualClock fakeClock;

    private class ManualClock extends TaskClock {
        private CountDownLatch latch = new CountDownLatch(1);

        public void start() { }

        public void await() {
            try {
                latch.await();
            } catch(Exception e) {
                fail();
            }
        }

        public void stop() {
            try {
                latch.countDown();
            } catch(Exception e) {
                fail();
            }
        }

        public void elapseTime() {
            super.listener.run();
        }
    }

    @Rule public MockitoRule mockitoRule = MockitoJUnit.rule();
    @Mock public KafkaProducer kp;
    @Captor ArgumentCaptor<ProducerRecord<byte[], OpenTSDB.RollupMessage>> recordCaptor;

    @Before
    public void setUp() {
        KafkaConfig cfg = new KafkaConfig();
        cfg.setTopic("test");
        cfg.setServer("kafka:1000");
        cfg.setMsgPerPeriod(MESSAGES_PER_PERIOD);
        cfg.setQueueCapacity(QUEUE_CAPACITY);
        cfg.setDelayMs(DELAY_MS);
        cfg.setPeriodMs(PERIOD_MS);

        realRkp = new RollupKafkaProducer(cfg, kp);
        mockQueue = Mockito.spy(new LinkedBlockingQueue(QUEUE_CAPACITY));
        mockRkp = Mockito.spy(new RollupKafkaProducer(cfg, kp, mockQueue));

        fakeClock = new ManualClock();
        mockRkp.startClock(fakeClock);
    }

    @Test
    public void testMessagesThrottled() throws InterruptedException {
        for (int i = 0; i < QUEUE_CAPACITY; i++) {
            mockRkp.sendMessage(("metric " + i).getBytes(), OpenTSDB.RollupMessage.newBuilder().build());
        }

        fakeClock.elapseTime();
        Mockito.verify(kp, Mockito.times(MESSAGES_PER_PERIOD)).send(Mockito.any());
        fakeClock.elapseTime();
        Mockito.verify(kp, Mockito.times(MESSAGES_PER_PERIOD * 2)).send(Mockito.any());
        fakeClock.elapseTime();
        Mockito.verify(kp, Mockito.times(QUEUE_CAPACITY)).send(recordCaptor.capture());

        List<ProducerRecord<byte[], OpenTSDB.RollupMessage>> messages = recordCaptor.getAllValues();
        for (int i = 0; i < QUEUE_CAPACITY; i ++) {
            assertEquals("test", messages.get(i).topic());
            assertEquals(new String(("metric " + i).getBytes()), new String(messages.get(i).key()));
            assertEquals(messages.get(i).value().getClass(), OpenTSDB.RollupMessage.class);
        }
    }

    /**
     * Allows to send messages in a separate thread, useful for testing asynchronous behaviour.
     * It also wraps boilerplate.
     */
    private class SendMessage implements Runnable {

        public void run(){
            mockRkp.sendMessage(("metric").getBytes(), OpenTSDB.RollupMessage.newBuilder().build());
        }
    }

    @Test
    public void testWaitForMessageQueueSpace() throws InterruptedException {
        for (int i = 0; i < QUEUE_CAPACITY; i++) {
            new SendMessage().run();
        }
        Thread t1 = new Thread(new SendMessage());
        t1.start();
        Thread.sleep(200);
        assertEquals(Thread.State.WAITING, t1.getState());

        fakeClock.elapseTime();
        Thread.sleep(200);
        assertEquals(Thread.State.TERMINATED, t1.getState());
    }

    @Test
    public void testHandleExceptionForRemoveMessage() throws InterruptedException {
        new SendMessage().run();
        Mockito.verify(kp, Mockito.times(0)).send(Mockito.any());

        Mockito.doThrow(InterruptedException.class).when(mockQueue).take();
        fakeClock.elapseTime();
        Mockito.verify(kp, Mockito.times(0)).send(Mockito.any());

        Mockito.reset(mockQueue);
        fakeClock.elapseTime();
        // At the next tick of the clock, without errors, the last message is processed
        Mockito.verify(kp, Mockito.times(1)).send(Mockito.any());
    }


    @Test
    public void testAwaitMsgQueueProcessed() throws InterruptedException {
        /**
         * Allows to wait for the queue to be processed in a new thread, to test async behaviour.
         */
        class ProcessQueue implements Runnable {
            public void run() {
                new SendMessage().run();
                try {
                    mockRkp.await();
                } catch (Exception e) {
                    fail();
                }
            }
        }

        Thread t1 = new Thread(new ProcessQueue());
        t1.start();
        Thread.sleep(400);
        assertEquals(Thread.State.WAITING, t1.getState());

        fakeClock.stop();
        Thread.sleep(400);
        assertEquals(Thread.State.TERMINATED, t1.getState());

    }

    @Test
    public void testProducerClock() throws InterruptedException {
        realRkp.startClock();

        realRkp.sendMessage(("metric").getBytes(), OpenTSDB.RollupMessage.newBuilder().build());
        realRkp.sendMessage(("metric").getBytes(), OpenTSDB.RollupMessage.newBuilder().build());
        realRkp.sendMessage(("metric").getBytes(), OpenTSDB.RollupMessage.newBuilder().build());

        Thread.sleep(DELAY_MS + 100);
        Mockito.verify(kp, Mockito.times(2)).send(Mockito.any());


        Thread.sleep(PERIOD_MS + 100);
        Mockito.verify(kp, Mockito.times(3)).send(Mockito.any());
    }
}
