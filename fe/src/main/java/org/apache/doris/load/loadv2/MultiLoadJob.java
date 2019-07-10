// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

package org.apache.doris.load.loadv2;

import org.apache.doris.catalog.AuthorizationInfo;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.FailMsg;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.thrift.TSubLoadCommitRequest;
import org.apache.doris.transaction.BeginTransactionException;
import org.apache.doris.transaction.TabletCommitInfo;
import org.apache.doris.transaction.TransactionException;
import org.apache.doris.transaction.TransactionState;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An streaming multi load job.
 * The job of multi load contains many sub load.
 * Those sub load will be committed together. The multi load protects the atomicity of data in all sub loads.
 * The data in all sub loads must be loaded or unloaded together.
 * Multi load only supports the sub mini load right now.
 * It will support the sub stream load in the future.
 *
 * Every sub load in multi load is synchronous.
 * It means the status of response is 'Success' when sub load is successful and vice versa.
 *
 * User could check the multi load by stmt of show load.
 * But the show load could not perform the info of sub load.
 * User could check the sub labels by '_multi_desc'
 *
 */
public class MultiLoadJob extends LoadJob {
    private static final Logger LOG = LogManager.getLogger(MultiLoadJob.class);

    private Set<Long> tableIds = Sets.newConcurrentHashSet();
    private Map<String, SubLoadInfo> subLoadLabelToSubLoadInfo = Maps.newConcurrentMap();

    // only for log replay
    public MultiLoadJob() {
        this.jobType = EtlJobType.MULTI;
    }

    public MultiLoadJob(long dbId, String label) throws MetaNotFoundException {
        super(dbId, label);
        this.jobType = EtlJobType.MULTI;
        this.authorizationInfo = gatherAuthInfo();
    }

    public static MultiLoadJob fromMultiStart(long dbId, String label, Map<String, String> properties)
            throws DdlException, MetaNotFoundException {
        MultiLoadJob multiLoadJob = new MultiLoadJob(dbId, label);
        multiLoadJob.setJobProperties(properties);
        return multiLoadJob;
    }

    public AuthorizationInfo gatherAuthInfo() throws MetaNotFoundException {
        Database database = Catalog.getCurrentCatalog().getDb(dbId);
        if (database == null) {
            throw new MetaNotFoundException("Database " + dbId + "has been deleted");
        }
        return new AuthorizationInfo(database.getFullName(), getTableNames());
    }

    public List<String> getSubLabels() {
        return Lists.newArrayList(subLoadLabelToSubLoadInfo.keySet());
    }

    public long addSubLoad(String subLabel, String tableName, long createTimestamp)
            throws DdlException, MetaNotFoundException {
        // step1: check database and table
        Database database = Catalog.getCurrentCatalog().getDb(dbId);
        if (database == null) {
            throw new MetaNotFoundException("Database " + dbId + " has been deleted");
        }
        database.readLock();
        Table table;
        try {
            table = database.getTable(tableName);
            if (table == null) {
                throw new MetaNotFoundException("There is no table " + tableName
                                                        + " in database " + database.getFullName());
            }
        } finally {
            database.readUnlock();
        }

        writeLock();
        try {
            // step2: check sub load
            if (isCommitting) {
                throw new DdlException("The new sub load is forbidden when multi load is committing");
            }
            if (isCompleted()) {
                throw new DdlException("The new sub load is forbidden when multi load is completed");
            }
            SubLoadInfo subLoadInfo = subLoadLabelToSubLoadInfo.get(subLabel);
            if (subLoadInfo != null) {
                if (subLoadInfo.getCreateTimestamp() == createTimestamp) {
                    LOG.info(new LogBuilder(LogKey.LOAD_JOB, id)
                                     .add("sub_label", subLabel)
                                     .add("msg", "this is a duplicated request for sub load")
                                     .build());
                    return transactionId;
                }
                throw new LabelAlreadyUsedException("Sub label " + subLabel + " already has been used in load " +
                                                            label);
            }
            // step3: add sub load
            tableIds.add(table.getId());
            subLoadInfo = new SubLoadInfo(table.getId(), createTimestamp);
            subLoadLabelToSubLoadInfo.put(subLabel, subLoadInfo);
            return transactionId;
        } finally {
            writeUnlock();
        }
    }

    public void commitSubLoad(TSubLoadCommitRequest request) throws LoadException {
        writeLock();
        try {
            // step1: check state of multi load
            if (isCommitting) {
                throw new LoadException("The updating load status is forbidden when multi load is committing");
            }
            if (isCompleted()) {
                throw new LoadException("The updating load status is forbidden when multi load is completed");
            }
            // step2: check sub load in multi load
            SubLoadInfo subLoadInfo = subLoadLabelToSubLoadInfo.get(request.getSub_label());
            if (subLoadInfo == null) {
                throw new LoadException("There are no sub load named " + request.getSub_label() + " in load " + label);
            }
            // step3: update loading status
            if (request.isSetTracking_url() && request.getTracking_url() != null) {
                loadingStatus.addTrackingUrl(request.getTracking_url());
            }
            if (!request.isIs_successful()) {
                subLoadInfo.setErrorMsg(Strings.isNullOrEmpty(request.getError_msg()) ?
                                                "Unknown reason" : request.getError_msg());
                return;
            }
            loadingStatus.replaceCounter(DPP_ABNORMAL_ALL,
                                         increaseCounter(DPP_ABNORMAL_ALL, request.getAbnormal_rows()));
            loadingStatus.replaceCounter(DPP_NORMAL_ALL,
                                         increaseCounter(DPP_NORMAL_ALL, request.getNormal_rows()));
            subLoadInfo.setCommitInfoList(TabletCommitInfo.fromThrift(request.getCommit_info_list()));
        } finally {
            writeUnlock();
        }
    }

    private String increaseCounter(String key, long deltaValue) {
        long value = 0;
        if (loadingStatus.getCounters().containsKey(key)) {
            value = Long.valueOf(loadingStatus.getCounters().get(key));
        }
        value += Long.valueOf(deltaValue);
        return String.valueOf(value);
    }

    public void tryCommit() throws DdlException {
        // step1: merge commit info
        List<TabletCommitInfo> commitInfoList;
        writeLock();
        try {
            if (isCommitting) {
                return;
            }
            if (isCompleted()) {
                throw new DdlException("This operation is forbidden when multi load is completed");
            }
            if (subLoadLabelToSubLoadInfo.size() == 0) {
                throw new LoadException("Failed to commit multi load with no sub load. The multi load is cancelled");
            }
            if (!checkDataQuality()) {
                throw new LoadException(QUALITY_FAIL_MSG);
            }
            commitInfoList = mergeCommitInfo();
            isCommitting = true;
        } catch (LoadException e) {
            LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                             .add("database_id", dbId)
                             .add("error_msg", "Failed to commit multi load with error:" + e.getMessage())
                             .build(), e);
            // failed to commit multi load
            executeCancel(new FailMsg(FailMsg.CancelType.LOAD_RUN_FAIL, e.getMessage()), true);
            throw new DdlException(e.getMessage());
        } finally {
            writeUnlock();
        }

        // step2: commit and publish txn
        Database database = Catalog.getCurrentCatalog().getDb(dbId);
        if (database == null) {
            cancelJobWithoutCheck(new FailMsg(FailMsg.CancelType.LOAD_RUN_FAIL, "Database " + dbId + " has been "
                    + "deleted. The "
                    + "multi load is cancelled"), true);
        }
        try {
            if (!Catalog.getCurrentGlobalTransactionMgr().commitAndPublishTransaction(
                    database, transactionId, commitInfoList, getLeftTimeMs(),
                    new LoadJobFinalOperation(id, loadingStatus, progress, loadStartTimestamp,
                                              finishTimestamp, state, failMsg))) {
                throw new TransactionException("The load job was timeout when txn was committing and publishing");
            }
        } catch (UserException e) {
            LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                             .add("database_id", dbId)
                             .add("txn_id", transactionId)
                             .add("error_msg", "Failed to commit txn.")
                             .build(), e);
            cancelJobWithoutCheck(new FailMsg(FailMsg.CancelType.LOAD_RUN_FAIL, e.getMessage()), true);
            throw new DdlException(e.getMessage());
        }

    }

    /**
     * merge commit info list from different sub load
     * this operation effectively gather the final commit info so that its value is the
     * <i>intersection</i> of the sub loads.
     * Example:
     * sub load1: "commitInfoList": [{"tablet": 1, "backendId": 1,2,4}, {"tablet": 2, "backendId": 2,3,4}]
     * sub load2: "commitInfoList": [{"tablet": 1, "backendId": 1,2,3}, {"tablet": 3, "backendId": 2,3,4}]
     * sub load3: "commitInfoList": [{"tablet": 2, "backendId": 1,2,3}, {"tablet": 3, "backendId": 3}]
     * result: "commitInfoList": [{"tablet": 1, "backendId": 1,2}, {"tablet": 2, "backendId": 2,3},
     * {"tablet": 3, "backendId": 3}]
     *
     * @return
     * @throws LoadException when commit info could not be merged
     *                       Example:
     *                       sub load 1: "commitInfoList": [{"tablet": 1, "backendId": 1}]
     *                       merge sub load 2: "commitInfoList": [{"tablet": 1, "backendId": 2}]
     *                       result: throw LoadException
     *                       reason: If there don't have the same replica, the correct replica will not exist.
     */
    private List<TabletCommitInfo> mergeCommitInfo() throws LoadException {
        Map<Long, Set<Long>> tabletIdToCommitBackends = Maps.newHashMap();
        for (Map.Entry<String, SubLoadInfo> entity : subLoadLabelToSubLoadInfo.entrySet()) {
            SubLoadInfo subLoadInfo = entity.getValue();
            if (subLoadInfo.getCommitInfoList() == null && subLoadInfo.getErrorMsg() == null) {
                throw new LoadException("The multi load is cancelled because sub load "
                                                + entity.getKey() + " is unfinished");
            }
            if (subLoadInfo.getCommitInfoList() == null) {
                throw new LoadException("The multi load is cancelled because sub load "
                                                + entity.getKey() + " is failed with reason:"
                                                + subLoadInfo.getErrorMsg());
            }
            // get the tabletIdToCommitBackend in sub load
            Map<Long, Set<Long>> subLoadTabletIdToCommitBackends = Maps.newHashMap();
            for (TabletCommitInfo tabletCommitInfo : subLoadInfo.getCommitInfoList()) {
                Set<Long> commitBackends = subLoadTabletIdToCommitBackends.get(tabletCommitInfo.getTabletId());
                if (commitBackends == null) {
                    subLoadTabletIdToCommitBackends.put(tabletCommitInfo.getTabletId(),
                                                        Sets.newHashSet(tabletCommitInfo.getBackendId()));
                } else {
                    commitBackends.add(tabletCommitInfo.getBackendId());
                }
            }
            // combine commit info of load and commit info of sub load
            for (Map.Entry<Long, Set<Long>> subLoadEntry : subLoadTabletIdToCommitBackends.entrySet()) {
                Set<Long> loadCommitBackends = tabletIdToCommitBackends.get(subLoadEntry.getKey());
                Set<Long> subLoadCommitBackends = subLoadEntry.getValue();
                if (loadCommitBackends == null) {
                    tabletIdToCommitBackends.put(subLoadEntry.getKey(), subLoadCommitBackends);
                } else {
                    // find the intersection of commit backends between load and sub load
                    loadCommitBackends.retainAll(subLoadCommitBackends);
                    if (loadCommitBackends.isEmpty()) {
                        throw new LoadException("The tablet " + subLoadEntry.getKey() + "has no correct replica "
                                                        + "between different sub load");
                    }
                }
            }
        }

        // convert tabletIdToCommitBackends to result
        List<TabletCommitInfo> tabletCommitInfoList = Lists.newArrayList();
        for (Map.Entry<Long, Set<Long>> loadEntry : tabletIdToCommitBackends.entrySet()) {
            long tabletId = loadEntry.getKey();
            for (long backendId : loadEntry.getValue()) {
                TabletCommitInfo tabletCommitInfo = new TabletCommitInfo(tabletId, backendId);
                tabletCommitInfoList.add(tabletCommitInfo);
            }
        }
        return tabletCommitInfoList;
    }

    @Override
    public void beginTxn() throws LabelAlreadyUsedException, BeginTransactionException, AnalysisException {
        transactionId = Catalog.getCurrentGlobalTransactionMgr()
                .beginTransaction(dbId, label, -1, "FE: " + FrontendOptions.getLocalHostAddress(),
                                  TransactionState.LoadJobSourceType.BACKEND_STREAMING, id,
                                  timeoutSecond);
    }

    @Override
    public Set<String> getTableNames() throws MetaNotFoundException {
        Set<String> result = Sets.newHashSet();
        Database database = Catalog.getCurrentCatalog().getDb(dbId);
        if (database == null) {
            throw new MetaNotFoundException("Database " + dbId + "has been deleted");
        }
        for (long tableId : tableIds) {
            Table table = database.getTable(tableId);
            if (table == null) {
                throw new MetaNotFoundException("Failed to find table " + tableId + " in db " + dbId);
            }
        }
        return result;
    }

    @Override
    public Set<String> getTableNamesForShow() {
        Database database = Catalog.getCurrentCatalog().getDb(dbId);
        if (database == null) {
            return tableIds.stream().map(entity -> String.valueOf(entity)).collect(Collectors.toSet());
        }
        Set<String> result = Sets.newHashSet();
        for (long tableId : tableIds) {
            Table table = database.getTable(tableId);
            if (table == null) {
                result.add(String.valueOf(tableId));
            }
        }
        return result;
    }

    @Override
    protected void executeReplayOnAborted(TransactionState txnState) {
        unprotectReadEndOperation((LoadJobFinalOperation) txnState.getTxnCommitAttachment());
    }

    @Override
    protected void executeReplayOnVisible(TransactionState txnState) {
        unprotectReadEndOperation((LoadJobFinalOperation) txnState.getTxnCommitAttachment());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
    }
}
