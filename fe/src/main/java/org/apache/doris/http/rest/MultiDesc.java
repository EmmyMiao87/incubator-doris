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

package org.apache.doris.http.rest;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.http.ActionController;
import org.apache.doris.http.BaseRequest;
import org.apache.doris.http.BaseResponse;
import org.apache.doris.http.IllegalArgException;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.loadv2.LoadJob;
import org.apache.doris.load.loadv2.MultiLoadJob;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.service.ExecuteEnv;

import com.google.common.base.Strings;

import java.util.List;

import io.netty.handler.codec.http.HttpMethod;

// List all labels of one multi-load
// Only unfinished multi-load can be desc by this api
public class MultiDesc extends RestBaseAction {
    private static final String DB_KEY = "db";
    private static final String LABEL_KEY = "label";

    private ExecuteEnv execEnv;

    public MultiDesc(ActionController controller, ExecuteEnv execEnv) {
        super(controller);
        this.execEnv = execEnv;
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        ExecuteEnv executeEnv = ExecuteEnv.getInstance();
        MultiDesc action = new MultiDesc(controller, executeEnv);
        controller.registerHandler(HttpMethod.POST, "/api/{db}/_multi_desc", action);
    }

    @Override
    public void executeWithoutPassword(ActionAuthorizationInfo authInfo, BaseRequest request, BaseResponse response)
            throws DdlException {
        String db = request.getSingleParameter(DB_KEY);
        if (Strings.isNullOrEmpty(db)) {
            throw new DdlException("No database selected");
        }
        String label = request.getSingleParameter(LABEL_KEY);
        if (Strings.isNullOrEmpty(label)) {
            throw new DdlException("No label selected");
        }

        String fullDbName = ClusterNamespace.getFullName(authInfo.cluster, db);
        checkDbAuth(authInfo, fullDbName, PrivPredicate.LOAD);

        // only Master has these load info
        if (redirectToMaster(request, response)) {
            return;
        }

        try {
            LoadJob loadJob = Catalog.getCurrentCatalog().getLoadManager().getUnfinishedLoadJob(authInfo.cluster, db,
                                                                                                label);
            if (loadJob.getJobType() != EtlJobType.MULTI) {
                throw new DdlException("Only multi load can be desc by this api");
            }
            List<String> subLabels = ((MultiLoadJob) loadJob).getSubLabels();
            sendResult(request, response, new Result(subLabels));
        } catch (MetaNotFoundException e) {
            throw new DdlException(e.getMessage());
        }
    }

    private static class Result extends RestBaseResult {
        private List<String> labels;

        public Result(List<String> labels) {
            this.labels = labels;
        }
    }
}

