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

package net.skyscanner.opentsdb_rollups.config;

import java.util.List;

public class TimeFilterConfig {

    private boolean useHbaseServerTimeFilter;
    private boolean useMetricTimeRangeFilter;
    private long startTime;
    private long endTime;
    private long gracePeriodMs;
    private List<Integer> timestampsToFind;

    public boolean isUseHbaseServerTimeFilter() {
        boolean validTimestamps = startTime <= endTime;
        return useHbaseServerTimeFilter && validTimestamps;
    }

    public void setUseHbaseServerTimeFilter(boolean useHbaseServerTimeFilter) {
        this.useHbaseServerTimeFilter = useHbaseServerTimeFilter;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public long getGracePeriodMs() {
        return gracePeriodMs;
    }

    public void setGracePeriodMs(long gracePeriodMs) {
        this.gracePeriodMs = gracePeriodMs;
    }

    public boolean isUseMetricTimeRangeFilter() {
        return useMetricTimeRangeFilter;
    }

    public void setUseMetricTimeRangeFilter(boolean useMetricTimeRangeFilter) {
        this.useMetricTimeRangeFilter = useMetricTimeRangeFilter;
    }

    public List<Integer> getTimestampsToFind() {
        return timestampsToFind;
    }

    public void setTimestampsToFind(List<Integer> timestampsToFind) {
        this.timestampsToFind = timestampsToFind;
    }
}
