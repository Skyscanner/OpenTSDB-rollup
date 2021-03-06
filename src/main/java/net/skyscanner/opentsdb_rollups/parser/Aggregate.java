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

import java.io.Serializable;

public class Aggregate implements Serializable {

    private double sum;
    private double min;
    private double max;
    private long count;

    Aggregate(double sum, double min, double max, long count) {
        this.sum = sum;
        this.min = min;
        this.max = max;
        this.count = count;
    }

    public double getSum() { return sum; }
    public double getMin() { return min; }
    public double getMax() { return max; }
    public long getCount() { return count; }
}
