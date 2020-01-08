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

package net.skyscanner.opentsdb_rollups.filter;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import net.skyscanner.opentsdb_rollup.filters.generated.TimeFilterProto;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class MetricTimeComparator extends ByteArrayComparable implements Serializable {

    private final int referenceTimestamp, timestampLength;
    private final byte saltLength;
    private final byte uidLength;

    /**
     * Constructor.
     *
     * @param referenceTimestamp the referenceTimestamp to compare against
     * @param saltLength         the number of bytes used for salts
     * @param uidLength          the number of bytes used for UIDs
     */
    public MetricTimeComparator(int referenceTimestamp, int timestampLength, byte saltLength, byte uidLength) {
        super(Bytes.toBytes(referenceTimestamp));
        this.referenceTimestamp = referenceTimestamp;
        this.timestampLength = timestampLength;
        this.saltLength = saltLength;
        this.uidLength = uidLength;
    }

    public static MetricTimeComparator parseFrom(final byte[] pbBytes) throws DeserializationException {
        try {
            TimeFilterProto.MetricTimeComparator proto = TimeFilterProto.MetricTimeComparator.parseFrom(pbBytes);
            return new MetricTimeComparator(
                    proto.getReferenceTimestamp(),
                    proto.getTimestampLength(),
                    proto.getSaltLength().byteAt(0),
                    proto.getUidLength().byteAt(0));
        } catch (InvalidProtocolBufferException e) {
            throw new DeserializationException();
        }
    }

    public int getReferenceTimestamp() {
        return referenceTimestamp;
    }

    public int getTimestampLength() {
        return timestampLength;
    }

    public byte getSaltLength() {
        return saltLength;
    }

    public byte getUidLength() {
        return uidLength;
    }

    @Override
    public int compareTo(byte[] value, int offset, int length) {
        int tsOffset = offset + saltLength + uidLength;
        int ts = ByteBuffer.wrap(Arrays.copyOfRange(value, tsOffset, tsOffset + timestampLength)).getInt();
        return referenceTimestamp - ts;
    }

    @Override
    public byte[] toByteArray() {
        return TimeFilterProto.MetricTimeComparator.newBuilder()
                .setValue(ByteString.copyFrom(getValue()))
                .setReferenceTimestamp(referenceTimestamp)
                .setTimestampLength(timestampLength)
                .setSaltLength(ByteString.copyFrom(new byte[]{saltLength}))
                .setUidLength(ByteString.copyFrom(new byte[]{uidLength}))
                .build().toByteArray();
    }
}
