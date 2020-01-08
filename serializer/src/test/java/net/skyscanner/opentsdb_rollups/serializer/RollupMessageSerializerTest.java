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

import com.google.protobuf.InvalidProtocolBufferException;
import net.skyscanner.schemas.OpenTSDB;
import org.junit.Test;

import java.util.HashMap;

import static org.junit.Assert.*;

public class RollupMessageSerializerTest {
    @Test
    public void testSerializeMessage() throws InvalidProtocolBufferException {
        String metric = "metric";

        OpenTSDB.RollupMessage rollupMessage = RollupMessageSerializer.createRollupMessage(metric, new HashMap<>(), 10000000L, 5, 1, 4, 2);

        assertEquals(metric, rollupMessage.getMetric());
        assertNotNull(rollupMessage.getTagsMap());
        assertEquals(10000000L, rollupMessage.getHeader().getEventTimestamp().getUnixTimeMillis());
        assertEquals(5, rollupMessage.getSum(), 0);
        assertEquals(1, rollupMessage.getMin(), 0);
        assertEquals(4, rollupMessage.getMax(), 0);
        assertEquals(2, rollupMessage.getCount(), 0);
    }
}
