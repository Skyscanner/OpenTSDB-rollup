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

import com.google.protobuf.InvalidProtocolBufferException;
import net.skyscanner.opentsdb_rollup.filters.generated.TimeFilterProto;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;

import java.util.ArrayList;
import java.util.List;

public class TimeFilter extends RowFilter {

    public static final byte TIMESTAMP_LENGTH = 4;
    public static final byte SALT_LENGTH = 1;
    public static final byte METRIC_UID_LENGTH = 3;

    private MetricTimeComparator timeComparator;

    private TimeFilter(CompareOp op, int timestamp) {
        super(op, new MetricTimeComparator(timestamp, TIMESTAMP_LENGTH, SALT_LENGTH, METRIC_UID_LENGTH));
        this.timeComparator = (MetricTimeComparator) getComparator();
    }

    public static FilterList List(List<Integer> times) {
        List<Filter> filterList = new ArrayList<>();
        for (int time : times) {
            filterList.add(new TimeFilter(CompareOp.EQUAL, time));
        }
        return new FilterList(FilterList.Operator.MUST_PASS_ONE, filterList);
    }

    public static TimeFilter Start(int timestamp) {
        return new TimeFilter(CompareOp.GREATER_OR_EQUAL, timestamp);
    }

    public static TimeFilter End(int timestamp) {
        return new TimeFilter(CompareOp.LESS, timestamp);
    }

    public static FilterList StartAndEnd(int startTime, int endTime) {
        if (endTime < startTime) {
            throw new IllegalArgumentException("endTime must be later than startTime");
        }
        return new FilterList(TimeFilter.Start(startTime), TimeFilter.End(endTime));
    }

    public static TimeFilter parseFrom(final byte[] pbBytes) throws DeserializationException {
        try {
            TimeFilterProto.TimeFilter proto = TimeFilterProto.TimeFilter.parseFrom(pbBytes);
            CompareOp compareOp = CompareOp.valueOf(proto.getCompareOp().name());
            int timestamp = proto.getTimestamp();

            return new TimeFilter(compareOp, timestamp);

        } catch (InvalidProtocolBufferException e) {
            throw new DeserializationException(e);
        }
    }

    @Override
    public byte[] toByteArray() {
        return TimeFilterProto.TimeFilter.newBuilder()
                .setTimestamp(timeComparator.getReferenceTimestamp())
                .setCompareOp(TimeFilterProto.CompareType.valueOf(compareOp.name()))
                .build()
                .toByteArray();
    }

    public MetricTimeComparator getTimeComparator() {
        return timeComparator;
    }
}
