/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.doris.load.loadv2;

import org.apache.doris.transaction.TabletCommitInfo;

import java.util.List;
import java.util.Map;

public class BrokerLoadingTaskAttachment implements LoadLoadingTaskAttachment {

    private Map<String, Long> fileMap;
    private Map<String, String> counters;
    private String trackingUrl;
    private List<TabletCommitInfo> commitInfoList;

    public BrokerLoadingTaskAttachment(Map<String, Long> fileMap, Map<String, String> counters, String trackingUrl,
                                       List<TabletCommitInfo> commitInfoList) {
        this.fileMap = fileMap;
        this.trackingUrl = trackingUrl;
        this.counters = counters;
        this.commitInfoList = commitInfoList;
    }

    public String getCounter(String key) {
        return counters.get(key);
    }

    public String getTrackingUrl() {
        return trackingUrl;
    }

    public Map<String, Long> getFileMap() {
        return fileMap;
    }

    public List<TabletCommitInfo> getCommitInfoList() {
        return commitInfoList;
    }
}
