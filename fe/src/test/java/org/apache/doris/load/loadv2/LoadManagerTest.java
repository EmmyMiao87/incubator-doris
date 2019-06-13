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

import mockit.Deencapsulation;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;

import org.apache.doris.analysis.LabelName;
import org.apache.doris.analysis.LoadStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.load.EtlJobType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.List;
import java.util.Map;

public class LoadManagerTest {
    private LoadManager loadManager;
    private static final String methodName = "getIdToLoadJobs";

    @Before
    public void setUp() throws Exception {
        loadManager = new LoadManager(new LoadJobScheduler());
        LoadJob job1 = new InsertLoadJob("job1", 1L, 1L, System.currentTimeMillis());
        Deencapsulation.invoke(loadManager, "addLoadJob", job1);
    }

    @Test
    public void testCreateHadoopJob(@Mocked LoadJobScheduler loadJobScheduler,
                                    @Injectable LoadStmt stmt,
                                    @Injectable LabelName labelName,
                                    @Mocked Catalog catalog,
                                    @Injectable Database database,
                                    @Injectable BrokerLoadJob brokerLoadJob) {
        Map<Long, Map<String, List<LoadJob>>> dbIdToLabelToLoadJobs = Maps.newHashMap();
        Map<String, List<LoadJob>> labelToLoadJobs = Maps.newHashMap();
        String label1 = "label1";
        List<LoadJob> loadJobs = Lists.newArrayList();
        loadJobs.add(brokerLoadJob);
        labelToLoadJobs.put(label1, loadJobs);
        dbIdToLabelToLoadJobs.put(1L, labelToLoadJobs);
        loadManager = new LoadManager(loadJobScheduler);
        Deencapsulation.setField(loadManager, "dbIdToLabelToLoadJobs", dbIdToLabelToLoadJobs);
        new Expectations() {
            {
                stmt.getLabel();
                result = labelName;
                labelName.getLabelName();
                result = "label1";
                catalog.getDb(anyString);
                result = database;
                database.getId();
                result = 1L;
            }
        };

        try {
            loadManager.createLoadJobV1FromStmt(stmt, EtlJobType.HADOOP, System.currentTimeMillis());
            Assert.fail("duplicated label is not be allowed");
        } catch (LabelAlreadyUsedException e) {
            // successful
        } catch (DdlException e) {
            Assert.fail(e.getMessage());
        }

    }

    @Test
    public void testSerializationNormal() throws Exception {
        File file = serializeToFile(loadManager);

        LoadManager newLoadManager = deserializeFromFile(file);

        Map<Long, LoadJob> loadJobs = Deencapsulation.invoke(loadManager, methodName);
        Map<Long, LoadJob> newLoadJobs = Deencapsulation.invoke(newLoadManager, methodName);
        Assert.assertEquals(loadJobs, newLoadJobs);
    }

    @Test
    public void testSerializationWithJobRemoved() throws Exception {
        //make job1 don't serialize
        Config.label_keep_max_second = 1;
        Thread.sleep(2000);

        File file = serializeToFile(loadManager);

        LoadManager newLoadManager = deserializeFromFile(file);
        Map<Long, LoadJob> newLoadJobs = Deencapsulation.invoke(newLoadManager, methodName);

        Assert.assertEquals(0, newLoadJobs.size());
    }

    private File serializeToFile(LoadManager loadManager) throws Exception {
        File file = new File("./loadManagerTest");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));
        loadManager.write(dos);
        dos.flush();
        dos.close();
        return file;
    }

    private LoadManager deserializeFromFile(File file) throws Exception {
        DataInputStream dis = new DataInputStream(new FileInputStream(file));
        LoadManager loadManager = new LoadManager(new LoadJobScheduler());
        loadManager.readFields(dis);
        return loadManager;
    }
}
