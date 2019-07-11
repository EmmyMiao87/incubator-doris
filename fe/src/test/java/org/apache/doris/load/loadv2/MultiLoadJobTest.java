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

package org.apache.doris.load.loadv2;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.thrift.TSubLoadCommitRequest;
import org.apache.doris.thrift.TTabletCommitInfo;
import org.apache.doris.transaction.GlobalTransactionMgr;
import org.apache.doris.transaction.TabletCommitInfo;
import org.apache.doris.transaction.TxnCommitAttachment;

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import mockit.Verifications;

public class MultiLoadJobTest {

    @Test
    public void testTryCommitFailed(@Mocked Catalog catalog,
                                    @Injectable Database database,
                                    @Injectable Table table,
                                    @Injectable TSubLoadCommitRequest request1,
                                    @Injectable TSubLoadCommitRequest request2,
                                    @Injectable TTabletCommitInfo commitInfo1,
                                    @Injectable TTabletCommitInfo commitInfo2)
            throws MetaNotFoundException, DdlException, LoadException {
        List<TTabletCommitInfo> commitInfoList1 = Lists.newArrayList();
        commitInfoList1.add(commitInfo1);
        List<TTabletCommitInfo> commitInfoList2 = Lists.newArrayList();
        commitInfoList2.add(commitInfo2);
        new Expectations() {
            {
                catalog.getDb(anyLong);
                result = database;
                database.getTable(anyString);
                result = table;
                request1.getSub_label();
                result = "sub_label_1";
                request2.getSub_label();
                result = "sub_label_2";
                request1.getCommit_info_list();
                result = commitInfoList1;
                request2.getCommit_info_list();
                result = commitInfoList2;
                request1.isIs_successful();
                result = true;
                request2.isIs_successful();
                result = true;
                commitInfo1.getTabletId();
                result = 1;
                commitInfo1.getBackendId();
                result = 1;
                commitInfo2.getTabletId();
                result = 1;
                commitInfo2.getBackendId();
                result = 2;

            }
        };
        MultiLoadJob multiLoadJob = new MultiLoadJob(1, "label01");
        multiLoadJob.addSubLoad("sub_label_1", "table1", 1);
        multiLoadJob.addSubLoad("sub_label_2", "table1", 1);
        multiLoadJob.commitSubLoad(request1);
        multiLoadJob.commitSubLoad(request2);
        try {
            multiLoadJob.tryCommit();
            Assert.fail();
        } catch (DdlException e) {
            Assert.assertEquals(JobState.CANCELLED, multiLoadJob.getState());
        }
    }

    @Test
    public void testTryCommit(@Mocked Catalog catalog,
                              @Injectable Database database,
                              @Injectable Table table,
                              @Injectable TSubLoadCommitRequest request1,
                              @Injectable TSubLoadCommitRequest request2,
                              @Injectable TTabletCommitInfo commitInfo11,
                              @Injectable TTabletCommitInfo commitInfo12,
                              @Injectable TTabletCommitInfo commitInfo22,
                              @Injectable TTabletCommitInfo commitInfo23,
                              @Mocked GlobalTransactionMgr globalTransactionMgr)
            throws UserException {
        List<TTabletCommitInfo> commitInfoList1 = Lists.newArrayList();
        commitInfoList1.add(commitInfo11);
        commitInfoList1.add(commitInfo12);
        List<TTabletCommitInfo> commitInfoList2 = Lists.newArrayList();
        commitInfoList2.add(commitInfo22);
        commitInfoList2.add(commitInfo23);
        new Expectations() {
            {
                catalog.getDb(anyLong);
                result = database;
                database.getTable(anyString);
                result = table;
                request1.getSub_label();
                result = "sub_label_1";
                request2.getSub_label();
                result = "sub_label_2";
                request1.getCommit_info_list();
                result = commitInfoList1;
                request2.getCommit_info_list();
                result = commitInfoList2;
                request1.isIs_successful();
                result = true;
                request2.isIs_successful();
                result = true;
                commitInfo11.getTabletId();
                result = 1;
                commitInfo11.getBackendId();
                result = 2;
                commitInfo12.getTabletId();
                result = 1;
                commitInfo12.getBackendId();
                result = 2;
                commitInfo22.getTabletId();
                result = 1;
                commitInfo22.getBackendId();
                result = 2;
                commitInfo23.getTabletId();
                result = 1;
                commitInfo23.getBackendId();
                result = 3;

                globalTransactionMgr.commitAndPublishTransaction(database, anyLong, (List<TabletCommitInfo>) any,
                                                                 anyLong, (TxnCommitAttachment) any);
                result = true;
            }
        };
        MultiLoadJob multiLoadJob = new MultiLoadJob(1, "label01");
        multiLoadJob.addSubLoad("sub_label_1", "table1", 1);
        multiLoadJob.addSubLoad("sub_label_2", "table1", 1);
        multiLoadJob.commitSubLoad(request1);
        multiLoadJob.commitSubLoad(request2);
        List<TabletCommitInfo> result = Lists.newArrayList();
        TabletCommitInfo tabletCommitInfo = new TabletCommitInfo(1, 2);
        result.add(tabletCommitInfo);
        try {
            multiLoadJob.tryCommit();
        } catch (DdlException e) {
            Assert.fail();
        }
        new Verifications() {
            {
                List<TabletCommitInfo> tabletCommitInfoList = Lists.newArrayList();
                globalTransactionMgr.commitAndPublishTransaction(database, anyLong,
                                                     (List<TabletCommitInfo>) withCapture(tabletCommitInfoList),
                                                     anyLong, (TxnCommitAttachment) any);
                Assert.assertEquals(1, tabletCommitInfoList.size());
                Assert.assertEquals(1L, ((List<TabletCommitInfo>) tabletCommitInfoList.get(0)).get(0)
                        .getTabletId());
                Assert.assertEquals(2L, ((List<TabletCommitInfo>) tabletCommitInfoList.get(0)).get(0)
                        .getBackendId());
            }
        };
    }
}
