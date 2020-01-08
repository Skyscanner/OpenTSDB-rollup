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

public class HBaseConfig {

    private String tsdbTableSnapshotName, hbaseTableSnapshotInputFormatRestoreDir, hbaseRootdir, siteXmlPath;

    public String getTsdbTableSnapshotName() {
        return tsdbTableSnapshotName;
    }

    public void setTsdbTableSnapshotName(String tsdbTableSnapshotName) {
        this.tsdbTableSnapshotName = tsdbTableSnapshotName;
    }

    public String getHbaseTableSnapshotInputFormatRestoreDir() {
        return hbaseTableSnapshotInputFormatRestoreDir;
    }

    public void setHbaseTableSnapshotInputFormatRestoreDir(String hbaseTableSnapshotInputFormatRestoreDir) {
        this.hbaseTableSnapshotInputFormatRestoreDir = hbaseTableSnapshotInputFormatRestoreDir;
    }

    public String getHbaseRootdir() {
        return hbaseRootdir;
    }

    public void setHbaseRootdir(String hbaseRootdir) {
        this.hbaseRootdir = hbaseRootdir;
    }

    public String getSiteXmlPath() {
        return siteXmlPath;
    }

    public void setSiteXmlPath(String siteXmlPath) {
        this.siteXmlPath = siteXmlPath;
    }
}
