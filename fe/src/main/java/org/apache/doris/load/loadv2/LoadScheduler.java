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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.util.Daemon;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.load.FailMsg;
import org.apache.doris.transaction.BeginTransactionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/*
LoadScheduler will schedule the pending LoadJob which belongs to LoadManager.
Every pending job will be allocated one pending task.
The pre-loading checking will be executed by pending task.
If there is a pending task belong to pending job, job will not be rescheduled.
 */
public class LoadScheduler extends Daemon {

    private static final Logger LOG = LogManager.getLogger(LoadScheduler.class);

    private LoadManager loadManager;

    public LoadScheduler(LoadManager loadManager) {
        super();
        this.loadManager = loadManager;
    }

    @Override
    protected void runOneCycle() {
        try {
            process();
        } catch (Throwable e) {
            LOG.warn("Failed to process one round of RoutineLoadScheduler with error message {}", e.getMessage(), e);
        }
    }

    private void process() {
        // fetch all of pending job without pending task in loadManager
        List<LoadJob> loadJobList = loadManager.getNeedScheduleJobs();

        // the limit of job will be restrict when begin txn

        // schedule load job
        for (LoadJob loadJob : loadJobList) {
            try {
                // begin txn
                loadJob.beginTxn();
            } catch (LabelAlreadyUsedException | AnalysisException e) {
                LOG.warn(new LogBuilder(LogKey.LOAD_JOB, loadJob.getId())
                                 .add("error_msg", "There are error properties in job. Job will be cancelled")
                                 .build(), e);
                loadJob.updateState(JobState.CANCELLED, FailMsg.CancelType.ETL_SUBMIT_FAIL, e.getMessage());
                continue;
            } catch (BeginTransactionException e) {
                LOG.warn(new LogBuilder(LogKey.LOAD_JOB, loadJob.getId())
                                 .add("error_msg", "Failed to begin txn when job is scheduling. "
                                         + "Job will be rescheduled later")
                                 .build(), e);
                continue;
            }
            // divide job into pending task and add it to pool
            loadJob.divideToPendingTask();
        }
    }
}
