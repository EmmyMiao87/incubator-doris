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


import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.DataDescription;
import org.apache.doris.analysis.LoadStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.load.BrokerFileGroup;
import org.apache.doris.load.FailMsg;
import org.apache.doris.load.PullLoadSourceInfo;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * There are 3 steps in BrokerLoadJob: BrokerPendingTask, LoadLoadingTask, CommitAndPublishTxn.
 * Step1: BrokerPendingTask will be created on method of executeScheduleJob.
 * Step2: LoadLoadingTasks will be created by the method of onTaskFinished when BrokerPendingTask is finished.
 * Step3: CommitAndPublicTxn will be called by the method of onTaskFinished when all of LoadLoadingTasks are finished.
 */
public class BrokerLoadJob extends LoadJob {

    private static final Logger LOG = LogManager.getLogger(BrokerLoadJob.class);

    private BrokerDesc brokerDesc;
    // include broker desc and data desc
    private PullLoadSourceInfo dataSourceInfo = new PullLoadSourceInfo();

    // it will be set to true when pending task finished
    private boolean isLoading = false;

    public BrokerLoadJob(long dbId, String label, BrokerDesc brokerDesc, LoadManager loadManager) {
        super(dbId, label, loadManager);
        this.timeoutSecond = Config.pull_load_task_default_timeout_second;
        this.brokerDesc = brokerDesc;
    }

    public static BrokerLoadJob fromLoadStmt(LoadStmt stmt, LoadManager loadManager) throws DdlException {
        // get db id
        String dbName = stmt.getLabel().getDbName();
        Database db = Catalog.getCurrentCatalog().getDb(stmt.getLabel().getDbName());
        if (db == null) {
            throw new DdlException("Database[" + dbName + "] does not exist");
        }

        // create job
        BrokerLoadJob brokerLoadJob = new BrokerLoadJob(db.getId(), stmt.getLabel().getLabelName(),
                                                        stmt.getBrokerDesc(), loadManager);
        brokerLoadJob.setJobProperties(stmt.getProperties());
        brokerLoadJob.checkDataSourceInfo(db, stmt.getDataDescriptions());
        brokerLoadJob.setDataSourceInfo(db, stmt.getDataDescriptions());
        return brokerLoadJob;
    }

    private void setDataSourceInfo(Database db, List<DataDescription> dataDescriptions) throws DdlException {
        for (DataDescription dataDescription : dataDescriptions) {
            BrokerFileGroup fileGroup = new BrokerFileGroup(dataDescription);
            fileGroup.parse(db);
            dataSourceInfo.addFileGroup(fileGroup);
        }
    }

    @Override
    public void executeScheduleJob() {
        LoadTask task = new BrokerLoadPendingTask(this, dataSourceInfo.getIdToFileGroups(), brokerDesc);
        unprotectSubmitTask(task);
    }

    /**
     * Situation1: When attachment is instance of BrokerPendingTaskAttachment, this method is called by broker pending task.
     * LoadLoadingTask will be created after BrokerPendingTask is finished.
     * Situation2: When attachment is instance of BrokerLoadingTaskAttachment, this method is called by LoadLoadingTask.
     * CommitTxn will be called after all of LoadingTasks are finished.
     *
     * @param attachment
     */
    @Override
    public void onTaskFinished(TaskAttachment attachment) {
        if (attachment instanceof BrokerPendingTaskAttachment) {
            onPendingTaskFinished((BrokerPendingTaskAttachment) attachment);
        } else if (attachment instanceof BrokerLoadingTaskAttachment) {
            onLoadingTaskFinished((BrokerLoadingTaskAttachment) attachment);
        }
    }

    @Override
    public void onTaskFailed(String errMsg) {
        updateState(JobState.CANCELLED, FailMsg.CancelType.LOAD_RUN_FAIL, errMsg);
    }

    /**
     * step1: divide job into loading task
     * step2: init the plan of task
     * step3: submit tasks into loadingTaskExecutor
     * @param attachment BrokerPendingTaskAttachment
     */
    private void onPendingTaskFinished(BrokerPendingTaskAttachment attachment) {
        // TODO(ml): check if task has been cancelled
        writeLock();
        try {
            if (isLoading) {
                LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                                 .add("error_msg", "this is a duplicated callback of pending task "
                                         + "when broker already has loading task"));
                return;
            }
            isLoading = true;
        } finally {
            writeUnlock();
        }

        Database db = null;
        try {
            db = getDb();
        } catch (MetaNotFoundException e) {
            LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                             .add("database_id", dbId)
                             .add("error_msg", "Failed to divide job into loading task when db not found.")
                             .build(), e);
            updateState(JobState.CANCELLED, FailMsg.CancelType.ETL_RUN_FAIL, "db does not exist. id: " + dbId);
            return;
        }

        // divide job into broker loading task by table
        db.readLock();
        try {
            for (Map.Entry<Long, List<BrokerFileGroup>> entry :
                    dataSourceInfo.getIdToFileGroups().entrySet()) {
                long tableId = entry.getKey();
                OlapTable table = (OlapTable) db.getTable(tableId);
                if (table == null) {
                    LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                                     .add("database_id", dbId)
                                     .add("table_id", tableId)
                                     .add("error_msg", "Failed to divide job into loading task when table not found")
                                     .build());
                    updateState(JobState.CANCELLED, FailMsg.CancelType.ETL_RUN_FAIL,
                                "Unknown table(" + tableId + ") in database(" + db.getFullName() + ")");
                    return;
                }

                // Generate loading task and init the plan of task
                LoadLoadingTask task = new LoadLoadingTask(db, table, brokerDesc,
                                                           entry.getValue(), getDeadlineMs(), execMemLimit,
                                                           transactionId, this);
                task.init(attachment.getFileStatusByTable(tableId),
                          attachment.getFileNumByTable(tableId));
                // Add tasks into list and pool
                unprotectSubmitTask(task);
            }
        } catch (UserException e) {
            updateState(JobState.CANCELLED, FailMsg.CancelType.ETL_RUN_FAIL, "failed to " + e.getMessage());
        } finally {
            db.readUnlock();
        }
        loadStartTimestamp = System.currentTimeMillis();
    }

    private void onLoadingTaskFinished(BrokerLoadingTaskAttachment attachment) {
        // TODO(ml): check if task has been cancelled
        writeLock();
        try {
            // update loading status
            updateLoadingStatus(attachment);

            // begin commit txn when all of loading tasks have been finished
            if (!(tasks.size() == tasks.stream()
                    .filter(entity -> entity.isFinished()).count())) {
                return;
            }

            // check data quality
            if (!checkDataQuality()) {
                unprotectedUpdateState(JobState.CANCELLED, FailMsg.CancelType.ETL_QUALITY_UNSATISFIED,
                                       QUALITY_FAIL_MSG);
                return;
            }
        } finally {
            writeUnlock();
        }

        Database db = null;
        try {
            db = getDb();
        } catch (MetaNotFoundException e) {
            LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                             .add("database_id", dbId)
                             .add("error_msg", "Failed to commit txn when db not found.")
                             .build(), e);
            updateState(JobState.CANCELLED, FailMsg.CancelType.ETL_RUN_FAIL, "db does not exist. id: " + dbId);
            return;
        }
        try {
            Catalog.getCurrentGlobalTransactionMgr().commitAndPublishTransaction(
                    db, transactionId, commitInfos, getLeftTimeMs());
        } catch (UserException e) {
            updateState(JobState.CANCELLED, FailMsg.CancelType.LOAD_RUN_FAIL, "failed to " + e.getMessage());
        }
    }

    private void updateLoadingStatus(BrokerLoadingTaskAttachment attachment) {
        loadingStatus.replaceCounter(DPP_ABNORMAL_ALL,
                                     increaseCounter(DPP_ABNORMAL_ALL, attachment.getCounter(DPP_ABNORMAL_ALL)));
        loadingStatus.replaceCounter(DPP_NORMAL_ALL,
                                     increaseCounter(DPP_NORMAL_ALL, attachment.getCounter(DPP_NORMAL_ALL)));
        if (attachment.getTrackingUrl() != null) {
            loadingStatus.setTrackingUrl(attachment.getTrackingUrl());
        }
        commitInfos.addAll(attachment.getCommitInfoList());
        int finishedTaskNum = (int) tasks.stream().filter(entity -> entity.isFinished()).count();
        progress = finishedTaskNum / tasks.size() * 100;
        if (progress == 100) {
            progress = 99;
        }
    }

    private String increaseCounter(String key, String deltaValue) {
        long value = Long.valueOf(loadingStatus.getCounters().get(key));
        if (deltaValue != null) {
            value += Long.valueOf(deltaValue);
        }
        return String.valueOf(value);
    }
}
