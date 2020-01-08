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
import org.junit.Test;

import java.util.Map;

import static org.apache.commons.codec.binary.Hex.encodeHexString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TsdbTableParserTest {
    private final String SALT = "00";
    private final String METRIC_UID = "000001";
    private final String TIMESTAMP = "50E22700";

    private boolean equalStringBytes(String s, byte[] b) {
        return s.equals(encodeHexString(b));
    }

    private String mockTagKeyValue(int seed) throws Exception {
        if (seed > 9) throw new Exception("Seed must be single digit");
        return "00000" + seed + "00000000";
    }

    @Test
    public void testParseTsdbTableRowSucceeds() throws ParseException {
        String tagk1 = "000001";
        String tagv1 = "00000001";
        String tagk2 = "000002";
        String tagv2 = "00000004";
        byte[] b = Bytes.fromHex(SALT + METRIC_UID + TIMESTAMP + tagk1 + tagv1 + tagk2 + tagv2);

        TsdbRowKey rowKey = TsdbTableParser.parseTsdbTableRow(b);

        assertEquals(METRIC_UID, encodeHexString(rowKey.getMetricUid()));
        assertEquals((int) Integer.decode("0x" + TIMESTAMP), rowKey.getTimestamp());

        Map<byte[], byte[]> tags = rowKey.getTags();
        Map.Entry<byte[], byte[]> tagEntry1 = (Map.Entry<byte[], byte[]>) tags.entrySet().toArray()[0];
        Map.Entry<byte[], byte[]> tagEntry2 = (Map.Entry<byte[], byte[]>) tags.entrySet().toArray()[1];

        assertEquals(2, tags.size());
        boolean matchOneTwo = equalStringBytes(tagk1, tagEntry1.getKey()) &&
                equalStringBytes(tagk2, tagEntry2.getKey()) &&
                equalStringBytes(tagv1, tagEntry1.getValue()) &&
                equalStringBytes(tagv2, tagEntry2.getValue());
        boolean matchTwoOne = equalStringBytes(tagk1, tagEntry2.getKey()) &&
                equalStringBytes(tagk2, tagEntry1.getKey()) &&
                equalStringBytes(tagv1, tagEntry2.getValue()) &&
                equalStringBytes(tagv2, tagEntry1.getValue());

        assert (matchOneTwo || matchTwoOne);
    }

    @Test
    public void testParseTsdbTableRowWrongFormat() throws Exception {
        byte[] goodBytes = Bytes.fromHex(SALT + METRIC_UID + TIMESTAMP + mockTagKeyValue(1) + mockTagKeyValue(2) + mockTagKeyValue(3));
        TsdbRowKey goodRowKey = TsdbTableParser.parseTsdbTableRow(goodBytes);
        Map<byte[], byte[]> goodTags = goodRowKey.getTags();
        assertEquals(3, goodTags.size());

        byte[] badBytes = Bytes.fromHex(SALT + METRIC_UID + TIMESTAMP + "000001" + "000000000000");
        try {
            TsdbTableParser.parseTsdbTableRow(badBytes);
        } catch (ParseException e) {
            assertEquals("TSDB table row " + encodeHexString(badBytes) + " was an incorrect length", e.getMessage());
            return;
        }
        fail();
    }

    @Test
    public void testParseTsdbTableRowRepeatedTag() {
        String tagk = "000001";
        String tagv1 = "00000001";
        String tagv2 = "00000004";
        byte[] b = Bytes.fromHex(SALT + METRIC_UID + TIMESTAMP + tagk + tagv1 + tagk + tagv2);

        try {
            TsdbTableParser.parseTsdbTableRow(b);
        } catch (ParseException e) {
            assertEquals("TSDB table row had duplicate tag key " + tagk, e.getMessage());
            return;
        }
        fail();
    }
}
