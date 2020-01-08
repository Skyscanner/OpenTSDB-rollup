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

import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.*;

public class TimeFilterTest {

    private byte[] makeRowKey(int timestamp) {
        byte[] ts = Bytes.toBytes(timestamp);
        byte[] rowKey = new byte[TimeFilter.SALT_LENGTH + TimeFilter.METRIC_UID_LENGTH + TimeFilter.TIMESTAMP_LENGTH];

        System.arraycopy(ts, 0, rowKey, TimeFilter.SALT_LENGTH+ TimeFilter.METRIC_UID_LENGTH, ts.length);

        return rowKey;
    }

    @Test
    public void testListOfTimeFilters() throws IOException {
        Filter filter = TimeFilter.List(Arrays.asList(1561024800, 1561028400));

        assertRowIncluded(filter, makeRowKey(1561024800));
        assertRowIncluded(filter, makeRowKey(1561028400));
        assertRowExcluded(filter, makeRowKey(1561032000));
    }

    @Test
    public void testStartFilterFiltersCorrectly() throws IOException {
        final int referenceTS = 1561024800;
        Filter filter = TimeFilter.Start(referenceTS);

        assertRowIncluded(filter, makeRowKey(referenceTS + 1));   // just right
        assertRowIncluded(filter, makeRowKey(referenceTS));       // just right boundary
        assertRowExcluded(filter, makeRowKey(referenceTS - 1));   // too early
    }

    @Test
    public void testEndFilterFiltersCorrectly() throws IOException {
        final int referenceTS = 1561024800;
        Filter filter = TimeFilter.End(referenceTS);

        assertRowIncluded(filter, makeRowKey(referenceTS - 1));   // just right
        assertRowExcluded(filter, makeRowKey(referenceTS + 1));   // too late
        assertRowExcluded(filter, makeRowKey(referenceTS));       // too late boundary
    }

    @Test
    public void testListFilter() throws IOException {
        final int earlyTS = 1561040000;
        final int lateTS = 1561050000;

        Filter filter = TimeFilter.StartAndEnd(earlyTS, lateTS);

        byte[] tooEarly = makeRowKey(earlyTS - 1);
        byte[] tooLate = makeRowKey(lateTS + 1);
        byte[] tooLateBoundary = makeRowKey(lateTS);
        byte[] justRight = makeRowKey(lateTS - 1);
        byte[] justRightBoundary = makeRowKey(earlyTS);

        assertRowExcluded(filter, tooEarly);
        assertRowExcluded(filter, tooLate);
        assertRowExcluded(filter, tooLateBoundary);
        assertRowIncluded(filter, justRight);
        assertRowIncluded(filter, justRightBoundary);
    }


    /**
     * Assert the rows will be allowed to pass the filter
     */
    private static void assertRowIncluded(Filter filter, byte[] rowKey) throws IOException {
        assertFalse(filter.filterRowKey(rowKey, 0, 0));
        filter.reset();
    }

    /**
     * Assert the rows will be removed by the filter
     */
    private static void assertRowExcluded(Filter filter, byte[] rowKey) throws IOException {
        assertTrue(filter.filterRowKey(rowKey, 0, 0));
        filter.reset();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBeforeAndAfterThrows() {
        TimeFilter.StartAndEnd(2, 1);
    }

    @Test
    public void testFilterSerialisation() throws DeserializationException {
        TimeFilter filter = TimeFilter.Start(1560996000);

        byte[] encoded = filter.toByteArray();
        TimeFilter decoded = TimeFilter.parseFrom(encoded);

        assertEquals(filter.getOperator(), decoded.getOperator());
        assertEquals(filter.getTimeComparator().getReferenceTimestamp(), decoded.getTimeComparator().getReferenceTimestamp());
    }

    @Test
    public void testMetricTimeComparatorSerialisation() throws DeserializationException {
        int referenceTS = 1561040000;
        MetricTimeComparator mtc = new MetricTimeComparator(referenceTS, TimeFilter.TIMESTAMP_LENGTH, TimeFilter.SALT_LENGTH, TimeFilter.METRIC_UID_LENGTH);

        byte[] encoded = mtc.toByteArray();
        MetricTimeComparator decoded = MetricTimeComparator.parseFrom(encoded);

        assertEquals(referenceTS, decoded.getReferenceTimestamp());
        assertEquals(TimeFilter.TIMESTAMP_LENGTH, decoded.getTimestampLength());
        assertEquals(TimeFilter.SALT_LENGTH, decoded.getSaltLength());
        assertEquals(TimeFilter.METRIC_UID_LENGTH, decoded.getUidLength());
    }
}

