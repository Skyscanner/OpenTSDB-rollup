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

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.util.NavigableMap;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TsdbRowValueTest {

    @Test
    public void testDuplicateCellQualifiersRemoved() {
        byte[] row = {0x00, 0x22, 0x35, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x01, 0x00, 0x00, 0x0b, 0x5e, 0x00, 0x00, 0x04, 0x00, 0x00, 0x0b, 0x08, 0x00, 0x00, 0x06, 0x00, 0x00, 0x0b, 0x7a, 0x00, 0x00, 0x04, 0x00, 0x00, 0x01, 0x0f};
        NavigableMap<byte[], byte[]> map = new TreeMap<>(Bytes.BYTES_COMPARATOR);
        byte[] key = {3, -53};
        byte[] value = {78, 29, 66, -54};
        map.put(key, value);
        byte[] key2 = {3, -49};
        byte[] value2 = {65, -61, -74, -60, -98, 0, 0, 0};
        map.put(key2, value2);

        Result result = mock(Result.class);
        when(result.getRow()).thenReturn(row);
        when(result.getFamilyMap(any(byte[].class))).thenReturn((map));

        Aggregate aggregate = TsdbRowValue.aggregateRow(result);
        assertEquals(1, aggregate.getCount());
    }
}
