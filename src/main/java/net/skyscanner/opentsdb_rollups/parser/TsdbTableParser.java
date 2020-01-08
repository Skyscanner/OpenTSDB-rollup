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

import net.opentsdb.core.Internal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.*;

import static org.apache.commons.codec.binary.Hex.encodeHexString;

public class TsdbTableParser implements Serializable {

    private static final Logger log = LoggerFactory.getLogger(TsdbTableParser.class);

    // TODO: Refactor this to make it configurable.
    public static final byte SALT_LENGTH = 1;
    public static final byte METRIC_UID_LENGTH = 3;
    public static final byte TIMESTAMP_LENGTH = 4;

    private static final int TAGK_LENGTH = 3;
    private static final int TAGV_LENGTH = 4;

    /**
     * Parse the row key of the tsdb table * @param byteKey - row key in bytes
     * @return the parsed {@link TsdbRowKey}
     */
    public static TsdbRowKey parseTsdbTableRow(byte[] byteKey) throws ParseException {
        HashMap<byte[], byte[]> tags = new LinkedHashMap<>();

        int pos = 0;
        pos += SALT_LENGTH;  // Don't care about the salt
        byte[] metricUid = Arrays.copyOfRange(byteKey, pos, pos+=METRIC_UID_LENGTH);
        int timestamp = ByteBuffer.wrap(Arrays.copyOfRange(byteKey, pos, pos+=TIMESTAMP_LENGTH)).getInt();

        if ((byteKey.length - SALT_LENGTH - METRIC_UID_LENGTH - TIMESTAMP_LENGTH)
                % (TAGK_LENGTH + TAGV_LENGTH) != 0) {
            throw new ParseException("TSDB table row " + encodeHexString(byteKey) + " was an incorrect length");
        }

        int keyLimit = byteKey.length - TAGK_LENGTH - TAGV_LENGTH + 1;
        Set<String> tagKeys = new HashSet<>();
        for (int i = pos; i < keyLimit; i += TAGK_LENGTH + TAGV_LENGTH) {
            int tagPos = i;
            byte[] tagK = Arrays.copyOfRange(byteKey, tagPos, tagPos+=TAGK_LENGTH);
            byte[] tagV = Arrays.copyOfRange(byteKey, tagPos, tagPos + TAGV_LENGTH);

            if (tagKeys.contains(encodeHexString(tagK))) {
                throw new ParseException("TSDB table row had duplicate tag key " + encodeHexString(tagK));
            } else {
                tagKeys.add(encodeHexString(tagK));
            }

            tags.put(tagK, tagV);
        }

        return new TsdbRowKey(metricUid, timestamp, tags);
    }
}
