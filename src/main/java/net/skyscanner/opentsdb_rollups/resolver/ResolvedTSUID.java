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

package net.skyscanner.opentsdb_rollups.resolver;

import java.util.Map;

public class ResolvedTSUID {
    private final String metric;
    private final Map<String, String> tags;

    public ResolvedTSUID(String metric, Map<String, String> tags) {
        this.metric = metric;
        this.tags = tags;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public String getMetric() {
        return metric;
    }
}
