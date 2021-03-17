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

package org.apache.doris.qe;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.CreateTableAsSelectStmt;
import org.apache.doris.analysis.DdlStmt;
import org.apache.doris.analysis.EnterStmt;
import org.apache.doris.analysis.ExportStmt;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.InsertStmt;
import org.apache.doris.analysis.KillStmt;
import org.apache.doris.analysis.OutFileClause;
import org.apache.doris.analysis.QueryStmt;
import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.analysis.SelectStmt;
import org.apache.doris.analysis.SetStmt;
import org.apache.doris.analysis.SetVar;
import org.apache.doris.analysis.ShowStmt;
import org.apache.doris.analysis.SqlParser;
import org.apache.doris.analysis.SqlScanner;
import org.apache.doris.analysis.StatementBase;
import org.apache.doris.analysis.StmtRewriter;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.TransactionBeginStmt;
import org.apache.doris.analysis.TransactionCommitStmt;
import org.apache.doris.analysis.TransactionRollbackStmt;
import org.apache.doris.analysis.TransactionStmt;
import org.apache.doris.analysis.UnsupportedStmt;
import org.apache.doris.analysis.UseStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Table.TableType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.Version;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.MetaLockUtils;
import org.apache.doris.common.util.ProfileManager;
import org.apache.doris.common.util.QueryPlannerProfile;
import org.apache.doris.common.util.RuntimeProfile;
import org.apache.doris.common.util.SqlParserUtils;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlEofPacket;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.planner.Planner;
import org.apache.doris.planner.StreamLoadPlanner;
import org.apache.doris.proto.PExecPlanFragmentResult;
import org.apache.doris.proto.PQueryStatistics;
import org.apache.doris.proto.PStatus;
import org.apache.doris.proto.PUniqueId;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.cache.Cache;
import org.apache.doris.qe.cache.CacheAnalyzer;
import org.apache.doris.qe.cache.CacheAnalyzer.CacheMode;
import org.apache.doris.qe.cache.CacheBeProxy;
import org.apache.doris.qe.cache.CacheProxy;
import org.apache.doris.rewrite.ExprRewriter;
import org.apache.doris.rewrite.mvrewrite.MVSelectFailedException;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.system.Backend;
import org.apache.doris.task.LoadEtlTask;
import org.apache.doris.task.StreamLoadTask;
import org.apache.doris.thrift.TExecPlanFragmentParams;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TLoadTxnBeginRequest;
import org.apache.doris.thrift.TLoadTxnBeginResult;
import org.apache.doris.thrift.TMergeType;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TQueryOptions;
import org.apache.doris.thrift.TQueryType;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TStreamLoadPutRequest;
import org.apache.doris.thrift.TTxnParams;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.thrift.TWaitingTxnStatusRequest;
import org.apache.doris.transaction.TabletCommitInfo;
import org.apache.doris.transaction.TransactionCommitFailedException;
import org.apache.doris.transaction.TransactionEntry;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionStatus;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.io.IOException;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

// Do one COM_QUERY process.
// first: Parse receive byte array to statement struct.
// second: Do handle function for statement.
public class StmtExecutor {
    private static final Logger LOG = LogManager.getLogger(StmtExecutor.class);

    private static final AtomicLong STMT_ID_GENERATOR = new AtomicLong(0);
    private static final int MAX_DATA_TO_SEND_FOR_TXN = 100;

    private ConnectContext context;
    private MysqlSerializer serializer;
    private OriginStatement originStmt;
    private StatementBase parsedStmt;
    private Analyzer analyzer;
    private RuntimeProfile profile;
    private RuntimeProfile summaryProfile;
    private volatile Coordinator coord = null;
    private MasterOpExecutor masterOpExecutor = null;
    private RedirectStatus redirectStatus = null;
    private Planner planner;
    private boolean isProxy;
    private ShowResultSet proxyResultSet = null;
    private PQueryStatistics statisticsForAuditLog;
    private boolean isCached;

    private QueryPlannerProfile plannerProfile = new QueryPlannerProfile();

    // this constructor is mainly for proxy
    public StmtExecutor(ConnectContext context, OriginStatement originStmt, boolean isProxy) {
        this.context = context;
        this.originStmt = originStmt;
        this.serializer = context.getSerializer();
        this.isProxy = isProxy;
    }

    // this constructor is only for test now.
    public StmtExecutor(ConnectContext context, String stmt) {
        this(context, new OriginStatement(stmt, 0), false);
    }

    // constructor for receiving parsed stmt from connect processor
    public StmtExecutor(ConnectContext ctx, StatementBase parsedStmt) {
        this.context = ctx;
        this.parsedStmt = parsedStmt;
        this.originStmt = parsedStmt.getOrigStmt();
        this.serializer = context.getSerializer();
        this.isProxy = false;
    }

    // At the end of query execution, we begin to add up profile
    public void initProfile(QueryPlannerProfile plannerProfile) {
        // Summary profile
        profile = new RuntimeProfile("Query");
        summaryProfile = new RuntimeProfile("Summary");
        summaryProfile.addInfoString(ProfileManager.QUERY_ID, DebugUtil.printId(context.queryId()));
        summaryProfile.addInfoString(ProfileManager.START_TIME, TimeUtils.longToTimeString(context.getStartTime()));

        long currentTimestamp = System.currentTimeMillis();
        long totalTimeMs = currentTimestamp - context.getStartTime();
        summaryProfile.addInfoString(ProfileManager.END_TIME, TimeUtils.longToTimeString(currentTimestamp));
        summaryProfile.addInfoString(ProfileManager.TOTAL_TIME, DebugUtil.getPrettyStringMs(totalTimeMs));

        summaryProfile.addInfoString(ProfileManager.QUERY_TYPE, "Query");
        summaryProfile.addInfoString(ProfileManager.QUERY_STATE, context.getState().toString());
        summaryProfile.addInfoString("Doris Version", Version.DORIS_BUILD_VERSION);
        summaryProfile.addInfoString(ProfileManager.USER, context.getQualifiedUser());
        summaryProfile.addInfoString(ProfileManager.DEFAULT_DB, context.getDatabase());
        summaryProfile.addInfoString(ProfileManager.SQL_STATEMENT, originStmt.originStmt);
        summaryProfile.addInfoString(ProfileManager.IS_CACHED, isCached ? "Yes" : "No");

        RuntimeProfile plannerRuntimeProfile = new RuntimeProfile("Execution Summary");
        plannerProfile.initRuntimeProfile(plannerRuntimeProfile);
        summaryProfile.addChild(plannerRuntimeProfile);

        profile.addChild(summaryProfile);

        if (coord != null) {
            coord.getQueryProfile().getCounterTotalTime().setValue(TimeUtils.getEstimatedTime(plannerProfile.getQueryBeginTime()));
            coord.endProfile();
            profile.addChild(coord.getQueryProfile());
            coord = null;
        }
    }

    public Planner planner() {
        return planner;
    }

    public boolean isForwardToMaster() {
        if (Catalog.getCurrentCatalog().isMaster()) {
            return false;
        }

        // this is a query stmt, but this non-master FE can not read, forward it to master
        if ((parsedStmt instanceof QueryStmt) && !Catalog.getCurrentCatalog().isMaster()
                && !Catalog.getCurrentCatalog().canRead()) {
            return true;
        }

        if (redirectStatus == null) {
            return false;
        } else {
            return redirectStatus.isForwardToMaster();
        }
    }

    public ByteBuffer getOutputPacket() {
        if (masterOpExecutor == null) {
            return null;
        } else {
            return masterOpExecutor.getOutputPacket();
        }
    }

    public ShowResultSet getProxyResultSet() {
        return proxyResultSet;
    }

    public ShowResultSet getShowResultSet() {
        if (masterOpExecutor == null) {
            return null;
        } else {
            return masterOpExecutor.getProxyResultSet();
        }
    }

    public boolean isQueryStmt() {
        return parsedStmt != null && parsedStmt instanceof QueryStmt;
    }

    public StatementBase getParsedStmt() {
        return parsedStmt;
    }

    // query with a random sql
    public void execute() throws Exception {
        UUID uuid = UUID.randomUUID();
        TUniqueId queryId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        execute(queryId);
    }

    // Execute one statement with queryId
    // The queryId will be set in ConnectContext
    // This queryId will also be send to master FE for exec master only query.
    // query id in ConnectContext will be changed when retry exec a query or master FE return a different one.
    // Exception:
    //  IOException: talk with client failed.
    public void execute(TUniqueId queryId) throws Exception {

        plannerProfile.setQueryBeginTime();
        context.setStmtId(STMT_ID_GENERATOR.incrementAndGet());

        context.setQueryId(queryId);

        try {
            if (context.isTxnModel() && !(parsedStmt instanceof InsertStmt)
                    && !(parsedStmt instanceof TransactionStmt)) {
                throw new TException("This is in a transaction, only insert, commit, rollback is acceptable.");
            }
            // support select hint e.g. select /*+ SET_VAR(query_timeout=1) */ sleep(3);
            analyzeVariablesInStmt();

            if (!context.isTxnModel()) {
                // analyze this query
                analyze(sessionVariable.toThrift());
                if (isForwardToMaster()) {
                    forwardToMaster();
                    if (masterOpExecutor != null && masterOpExecutor.getQueryId() != null) {
                        context.setQueryId(masterOpExecutor.getQueryId());
                    }
                    return;
                } else {
                    LOG.debug("no need to transfer to Master. stmt: {}", context.getStmtId());
                }
            } else {
                analyzer = new Analyzer(context.getCatalog(), context);
                parsedStmt.analyze(analyzer);
            }

            if (parsedStmt instanceof QueryStmt) {
                context.getState().setIsQuery(true);
                int retryTime = Config.max_query_retry_time;
                for (int i = 0; i < retryTime; i ++) {
                    try {
                        //reset query id for each retry
                        if (i > 0) {
                            UUID uuid = UUID.randomUUID();
                            TUniqueId newQueryId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
                            LOG.warn("Query {} {} times with new query id: {}", DebugUtil.printId(queryId), i, newQueryId);
                            context.setQueryId(newQueryId);
                        }
                        handleQueryStmt();
                        if (context.getSessionVariable().isReportSucc()) {
                            writeProfile();
                        }
                        break;
                    } catch (RpcException e) {
                        if (i == retryTime - 1) {
                            throw e;
                        }
                        if (!context.getMysqlChannel().isSend()) {
                            LOG.warn("retry {} times. stmt: {}", (i + 1), parsedStmt.getOrigStmt().originStmt);
                        } else {
                            throw e;
                        }
                    } finally {
                        QeProcessorImpl.INSTANCE.unregisterQuery(context.queryId());
                    }
                }
            } else if (parsedStmt instanceof SetStmt) {
                handleSetStmt();
            } else if (parsedStmt instanceof EnterStmt) {
                handleEnterStmt();
            } else if (parsedStmt instanceof UseStmt) {
                handleUseStmt();
            } else if (parsedStmt instanceof TransactionStmt) {
                handleTransactionStmt();
            } else if (parsedStmt instanceof CreateTableAsSelectStmt) {
                handleInsertStmt();
            } else if (parsedStmt instanceof InsertStmt) { // Must ahead of DdlStmt because InserStmt is its subclass
                try {
                    handleInsertStmt();
                    if (context.getSessionVariable().isReportSucc()) {
                        writeProfile();
                    }
                } catch (Throwable t) {
                    LOG.warn("handle insert stmt fail", t);
                    // the transaction of this insert may already begun, we will abort it at outer finally block.
                    throw t;
                } finally {
                    QeProcessorImpl.INSTANCE.unregisterQuery(context.queryId());
                }
            } else if (parsedStmt instanceof DdlStmt) {
                handleDdlStmt();
            } else if (parsedStmt instanceof ShowStmt) {
                handleShow();
            } else if (parsedStmt instanceof KillStmt) {
                handleKill();
            } else if (parsedStmt instanceof ExportStmt) {
                handleExportStmt();
            } else if (parsedStmt instanceof UnsupportedStmt) {
                handleUnsupportedStmt();
            } else {
                context.getState().setError("Do not support this query.");
            }
        } catch (IOException e) {
            LOG.warn("execute IOException ", e);
            // the exception happens when interact with client
            // this exception shows the connection is gone
            context.getState().setError(e.getMessage());
            throw e;
        } catch (UserException e) {
            // analysis exception only print message, not print the stack
            LOG.warn("execute Exception. {}", e);
            context.getState().setError(e.getMessage());
            context.getState().setErrType(QueryState.ErrType.ANALYSIS_ERR);
        } catch (Exception e) {
            LOG.warn("execute Exception", e);
            context.getState().setError(e.getMessage());
            if (parsedStmt instanceof KillStmt) {
                // ignore kill stmt execute err(not monitor it)
                context.getState().setErrType(QueryState.ErrType.ANALYSIS_ERR);
            }
        } finally {
            if (!context.isTxnModel() && parsedStmt instanceof InsertStmt) {
                InsertStmt insertStmt = (InsertStmt) parsedStmt;
                // The transaction of a insert operation begin at analyze phase.
                // So we should abort the transaction at this finally block if it encounter exception.
                if (insertStmt.isTransactionBegin() && context.getState().getStateType() == MysqlStateType.ERR) {
                    try {
                        String errMsg = Strings.emptyToNull(context.getState().getErrorMessage());
                        Catalog.getCurrentGlobalTransactionMgr().abortTransaction(
                                insertStmt.getDbObj().getId(), insertStmt.getTransactionId(),
                                (errMsg == null ? "unknown reason" : errMsg));
                    } catch (Exception abortTxnException) {
                        LOG.warn("errors when abort txn", abortTxnException);
                    }
                }
            }
        }

    }

    private void analyzeVariablesInStmt() throws DdlException {
        SessionVariable sessionVariable = context.getSessionVariable();
        if (parsedStmt != null && parsedStmt instanceof SelectStmt) {
            SelectStmt selectStmt = (SelectStmt) parsedStmt;
            Map<String, String> optHints = selectStmt.getSelectList().getOptHints();
            if (optHints != null) {
                for (String key : optHints.keySet()) {
                    VariableMgr.setVar(sessionVariable, new SetVar(key, new StringLiteral(optHints.get(key))));
                }
            }
        }
    }

    private void forwardToMaster() throws Exception {
        boolean isQuery = parsedStmt instanceof QueryStmt;
        masterOpExecutor = new MasterOpExecutor(originStmt, context, redirectStatus, isQuery);
        LOG.debug("need to transfer to Master. stmt: {}", context.getStmtId());
        masterOpExecutor.execute();
    }

    private void writeProfile() {
        initProfile(plannerProfile);
        profile.computeTimeInChildProfile();
        StringBuilder builder = new StringBuilder();
        profile.prettyPrint(builder, "");
        ProfileManager.getInstance().pushProfile(profile);
    }

    // Analyze one statement to structure in memory.
    public void analyze(TQueryOptions tQueryOptions) throws UserException {
        LOG.info("begin to analyze stmt: {}, forwarded stmt id: {}", context.getStmtId(), context.getForwardedStmtId());

        // parsedStmt may already by set when constructing this StmtExecutor();
        if (parsedStmt == null) {
            // Parse statement with parser generated by CUP&FLEX
            SqlScanner input = new SqlScanner(new StringReader(originStmt.originStmt), context.getSessionVariable().getSqlMode());
            SqlParser parser = new SqlParser(input);
            try {
                parsedStmt = SqlParserUtils.getStmt(parser, originStmt.idx);
                parsedStmt.setOrigStmt(originStmt);
                parsedStmt.setUserInfo(context.getCurrentUserIdentity());
            } catch (Error e) {
                LOG.info("error happened when parsing stmt {}, id: {}", originStmt, context.getStmtId(), e);
                throw new AnalysisException("sql parsing error, please check your sql");
            } catch (AnalysisException e) {
                String syntaxError = parser.getErrorMsg(originStmt.originStmt);
                LOG.info("analysis exception happened when parsing stmt {}, id: {}, error: {}",
                        originStmt, context.getStmtId(), syntaxError, e);
                if (syntaxError == null) {
                    throw  e;
                } else {
                    throw new AnalysisException(syntaxError, e);
                }
            } catch (Exception e) {
                // TODO(lingbin): we catch 'Exception' to prevent unexpected error,
                // should be removed this try-catch clause future.
                LOG.info("unexpected exception happened when parsing stmt {}, id: {}, error: {}",
                        originStmt, context.getStmtId(), parser.getErrorMsg(originStmt.originStmt), e);
                throw new AnalysisException("Unexpected exception: " + e.getMessage());
            }

            analyzeVariablesInStmt();
        }
        redirectStatus = parsedStmt.getRedirectStatus();

        // yiguolei: insert stmt's grammar analysis will write editlog, so that we check if the stmt should be forward to master here
        // if the stmt should be forward to master, then just return here and the master will do analysis again
        if (isForwardToMaster()) {
            return;
        }
        
        analyzer = new Analyzer(context.getCatalog(), context);
        // Convert show statement to select statement here
        if (parsedStmt instanceof ShowStmt) {
            SelectStmt selectStmt = ((ShowStmt) parsedStmt).toSelectStmt(analyzer);
            if (selectStmt != null) {
                parsedStmt = selectStmt;
            }
        }

        if (parsedStmt instanceof QueryStmt
                || parsedStmt instanceof InsertStmt
                || parsedStmt instanceof CreateTableAsSelectStmt) {
            Map<Long, Table> tableMap = Maps.newTreeMap();
            QueryStmt queryStmt;
            Set<String> parentViewNameSet = Sets.newHashSet();
            if (parsedStmt instanceof QueryStmt) {
                queryStmt = (QueryStmt) parsedStmt;
                queryStmt.getTables(analyzer, tableMap, parentViewNameSet);
            } else {
                InsertStmt insertStmt;
                if (parsedStmt instanceof InsertStmt) {
                    insertStmt = (InsertStmt) parsedStmt;
                } else {
                    insertStmt = ((CreateTableAsSelectStmt) parsedStmt).getInsertStmt();
                }
                insertStmt.getTables(analyzer, tableMap, parentViewNameSet);
            }
            // table id in tableList is in ascending order because that table map is a sorted map
            List<Table> tables = Lists.newArrayList(tableMap.values());
            MetaLockUtils.readLockTables(tables);
            try {
                analyzeAndGenerateQueryPlan(tQueryOptions);
            } catch (MVSelectFailedException e) {
                /**
                 * If there is MVSelectFailedException after the first planner, there will be error mv rewritten in query.
                 * So, the query should be reanalyzed without mv rewritten and planner again.
                 * Attention: Only error rewritten tuple is forbidden to mv rewrite in the second time.
                 */
                resetAnalyzerAndStmt();
                analyzeAndGenerateQueryPlan(tQueryOptions);
            } catch (UserException e) {
                throw e;
            } catch (Exception e) {
                LOG.warn("Analyze failed because ", e);
                throw new AnalysisException("Unexpected exception: " + e.getMessage());
            } finally {
                MetaLockUtils.readUnlockTables(tables);
            }
        } else {
            try {
                parsedStmt.analyze(analyzer);
            } catch (AnalysisException e) {
                throw e;
            } catch (Exception e) {
                LOG.warn("Analyze failed because ", e);
                throw new AnalysisException("Unexpected exception: " + e.getMessage());
            }
        }
    }

    private void analyzeAndGenerateQueryPlan(TQueryOptions tQueryOptions) throws UserException {
        parsedStmt.analyze(analyzer);
        if (parsedStmt instanceof QueryStmt || parsedStmt instanceof InsertStmt) {
            ExprRewriter rewriter = analyzer.getExprRewriter();
            rewriter.reset();
            if (context.getSessionVariable().isEnableFoldConstantByBe()) {
                // fold constant expr
                parsedStmt.foldConstant(rewriter);

            }
            boolean isExplain = parsedStmt.isExplain();
            boolean isVerbose = parsedStmt.isVerbose();
            // Apply expr and subquery rewrites.
            boolean reAnalyze = false;

            parsedStmt.rewriteExprs(rewriter);
            reAnalyze = rewriter.changed();
            if (analyzer.containSubquery()) {
                parsedStmt = StmtRewriter.rewrite(analyzer, parsedStmt);
                reAnalyze = true;
            }
            if (reAnalyze) {
                // The rewrites should have no user-visible effect. Remember the original result
                // types and column labels to restore them after the rewritten stmt has been
                // reset() and re-analyzed.
                List<Type> origResultTypes = Lists.newArrayList();
                for (Expr e: parsedStmt.getResultExprs()) {
                    origResultTypes.add(e.getType());
                }
                List<String> origColLabels =
                        Lists.newArrayList(parsedStmt.getColLabels());

                // Re-analyze the stmt with a new analyzer.
                analyzer = new Analyzer(context.getCatalog(), context);

                // query re-analyze
                parsedStmt.reset();
                parsedStmt.analyze(analyzer);

                // Restore the original result types and column labels.
                parsedStmt.castResultExprs(origResultTypes);
                parsedStmt.setColLabels(origColLabels);
                if (LOG.isTraceEnabled()) {
                    LOG.trace("rewrittenStmt: " + parsedStmt.toSql());
                }
                if (isExplain) parsedStmt.setIsExplain(isExplain, isVerbose);
            }
        }
        plannerProfile.setQueryAnalysisFinishTime();

        // create plan
        planner = new Planner();
        if (parsedStmt instanceof QueryStmt || parsedStmt instanceof InsertStmt) {
            planner.plan(parsedStmt, analyzer, tQueryOptions);
        } else {
            planner.plan(((CreateTableAsSelectStmt) parsedStmt).getInsertStmt(),
                    analyzer, new TQueryOptions());
        }
        // TODO(zc):
        // Preconditions.checkState(!analyzer.hasUnassignedConjuncts());

        plannerProfile.setQueryPlanFinishTime();
    }

    private void resetAnalyzerAndStmt() {
        analyzer = new Analyzer(context.getCatalog(), context);

        parsedStmt.reset();
    }

    // Because this is called by other thread
    public void cancel() {
        Coordinator coordRef = coord;
        if (coordRef != null) {
            coordRef.cancel();
        }
    }

    // Handle kill statement.
    private void handleKill() throws DdlException {
        KillStmt killStmt = (KillStmt) parsedStmt;
        long id = killStmt.getConnectionId();
        ConnectContext killCtx = context.getConnectScheduler().getContext(id);
        if (killCtx == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_NO_SUCH_THREAD, id);
        }
        if (context == killCtx) {
            // Suicide
            context.setKilled();
        } else {
            // Check auth
            // Only user itself and user with admin priv can kill connection
            if (!killCtx.getQualifiedUser().equals(ConnectContext.get().getQualifiedUser())
                    && !Catalog.getCurrentCatalog().getAuth().checkGlobalPriv(ConnectContext.get(),
                                                                              PrivPredicate.ADMIN)) {
                ErrorReport.reportDdlException(ErrorCode.ERR_KILL_DENIED_ERROR, id);
            }

            killCtx.kill(killStmt.isConnectionKill());
        }
        context.getState().setOk();
    }

    // Process set statement.
    private void handleSetStmt() {
        try {
            SetStmt setStmt = (SetStmt) parsedStmt;
            SetExecutor executor = new SetExecutor(context, setStmt);
            executor.execute();
        } catch (DdlException e) {
            // Return error message to client.
            context.getState().setError(e.getMessage());
            return;
        }
        context.getState().setOk();
    }

    // send values from cache.
    // return true if the meta fields has been sent, otherwise, return false.
    // the meta fields must be sent right before the first batch of data(or eos flag).
    // so if it has data(or eos is true), this method must return true.
    private boolean sendCachedValues(MysqlChannel channel, List<CacheProxy.CacheValue> cacheValues,
                                     SelectStmt selectStmt, boolean isSendFields, boolean isEos)
            throws Exception {
        RowBatch batch = null;
        boolean isSend = isSendFields;
        for (CacheBeProxy.CacheValue value : cacheValues) {
            batch = value.getRowBatch();
            if (!isSend) {
                // send meta fields before sending first data batch.
                sendFields(selectStmt.getColLabels(), exprToType(selectStmt.getResultExprs()));
                isSend = true;
            }
            for (ByteBuffer row : batch.getBatch().getRows()) {
                channel.sendOnePacket(row);
            }
            context.updateReturnRows(batch.getBatch().getRows().size());
        }

        if (isEos) {
            if (batch != null) {
                statisticsForAuditLog = batch.getQueryStatistics();
            }
            if (!isSend) {
                sendFields(selectStmt.getColLabels(), exprToType(selectStmt.getResultExprs()));
                isSend = true;
            }
            context.getState().setEof();
        }
        return isSend;
    }

    /**
     * Handle the SelectStmt via Cache.
     */
    private void handleCacheStmt(CacheAnalyzer cacheAnalyzer, MysqlChannel channel, SelectStmt selectStmt) throws Exception {
        RowBatch batch = null;
        CacheBeProxy.FetchCacheResult cacheResult = cacheAnalyzer.getCacheData();
        CacheMode mode = cacheAnalyzer.getCacheMode();
        SelectStmt newSelectStmt = selectStmt;
        boolean isSendFields = false;
        if (cacheResult != null) {
            isCached = true;
            if (cacheAnalyzer.getHitRange() == Cache.HitRange.Full) {
                sendCachedValues(channel, cacheResult.getValueList(), newSelectStmt, isSendFields, true);
                return;
            }
            // rewrite sql
            if (mode == CacheMode.Partition) {
                if (cacheAnalyzer.getHitRange() == Cache.HitRange.Left) {
                    isSendFields = sendCachedValues(channel, cacheResult.getValueList(), newSelectStmt, isSendFields, false);
                }
                newSelectStmt = cacheAnalyzer.getRewriteStmt();
                newSelectStmt.reset();
                analyzer = new Analyzer(context.getCatalog(), context);
                newSelectStmt.analyze(analyzer);
                planner = new Planner();
                planner.plan(newSelectStmt, analyzer, context.getSessionVariable().toThrift());
            }
        }

        coord = new Coordinator(context, analyzer, planner);
        QeProcessorImpl.INSTANCE.registerQuery(context.queryId(),
                new QeProcessorImpl.QueryInfo(context, originStmt.originStmt, coord));
        coord.exec();

        while (true) {
            batch = coord.getNext();
            if (batch.getBatch() != null) {
                cacheAnalyzer.copyRowBatch(batch);
                if (!isSendFields) {
                    sendFields(newSelectStmt.getColLabels(), exprToType(newSelectStmt.getResultExprs()));
                    isSendFields = true;
                }
                for (ByteBuffer row : batch.getBatch().getRows()) {
                    channel.sendOnePacket(row);
                }
                context.updateReturnRows(batch.getBatch().getRows().size());
            }
            if (batch.isEos()) {
                break;
            }
        }
        
        if (cacheResult != null && cacheAnalyzer.getHitRange() == Cache.HitRange.Right) {
            isSendFields = sendCachedValues(channel, cacheResult.getValueList(), newSelectStmt, isSendFields, false);
        }

        cacheAnalyzer.updateCache();

        if (!isSendFields) {
            sendFields(newSelectStmt.getColLabels(), exprToType(newSelectStmt.getResultExprs()));
            isSendFields = true;
        }

        statisticsForAuditLog = batch.getQueryStatistics();
        context.getState().setEof();
        return;
    }

    // Process a select statement.
    private void handleQueryStmt() throws Exception {
        // Every time set no send flag and clean all data in buffer
        context.getMysqlChannel().reset();
        QueryStmt queryStmt = (QueryStmt) parsedStmt;

        QueryDetail queryDetail = new QueryDetail(context.getStartTime(),
                                                  DebugUtil.printId(context.queryId()),
                                                  context.getStartTime(), -1, -1,
                                                  QueryDetail.QueryMemState.RUNNING,
                                                  context.getDatabase(),
                                                  originStmt.originStmt);
        context.setQueryDetail(queryDetail);
        QueryDetailQueue.addOrUpdateQueryDetail(queryDetail);

        if (queryStmt.isExplain()) {
            String explainString = planner.getExplainString(planner.getFragments(), queryStmt.isVerbose() ? TExplainLevel.VERBOSE: TExplainLevel.NORMAL.NORMAL);
            handleExplainStmt(explainString);
            return;
        }

        RowBatch batch;
        MysqlChannel channel = context.getMysqlChannel();
        boolean isOutfileQuery = queryStmt.hasOutFileClause();

        // Sql and PartitionCache
        CacheAnalyzer cacheAnalyzer = new CacheAnalyzer(context, parsedStmt, planner);
        if (cacheAnalyzer.enableCache() && !isOutfileQuery && queryStmt instanceof SelectStmt) {
            handleCacheStmt(cacheAnalyzer, channel, (SelectStmt) queryStmt);
            return;
        }

        // send result
        // 1. If this is a query with OUTFILE clause, eg: select * from tbl1 into outfile xxx,
        //    We will not send real query result to client. Instead, we only send OK to client with
        //    number of rows selected. For example:
        //          mysql> select * from tbl1 into outfile xxx;
        //          Query OK, 10 rows affected (0.01 sec)
        //
        // 2. If this is a query, send the result expr fields first, and send result data back to client.
        boolean isSendFields = false;
        coord = new Coordinator(context, analyzer, planner);
        QeProcessorImpl.INSTANCE.registerQuery(context.queryId(),
                new QeProcessorImpl.QueryInfo(context, originStmt.originStmt, coord));
        coord.exec();
        plannerProfile.setQueryScheduleFinishTime();
        while (true) {
            batch = coord.getNext();
            // for outfile query, there will be only one empty batch send back with eos flag
            if (batch.getBatch() != null) {
                // For some language driver, getting error packet after fields packet will be recognized as a success result
                // so We need to send fields after first batch arrived
                if (!isSendFields) {
                    if (!isOutfileQuery) {
                        sendFields(queryStmt.getColLabels(), exprToType(queryStmt.getResultExprs()));
                    } else {
                        sendFields(OutFileClause.RESULT_COL_NAMES, OutFileClause.RESULT_COL_TYPES);
                    }
                    isSendFields = true;
                }
                for (ByteBuffer row : batch.getBatch().getRows()) {
                    channel.sendOnePacket(row);
                }
                context.updateReturnRows(batch.getBatch().getRows().size());
            }
            if (batch.isEos()) {
                break;
            }
        }
        if (!isSendFields) {
            if (!isOutfileQuery) {
                sendFields(queryStmt.getColLabels(), exprToType(queryStmt.getResultExprs()));
            } else {
                sendFields(OutFileClause.RESULT_COL_NAMES, OutFileClause.RESULT_COL_TYPES);
            }
        }

        statisticsForAuditLog = batch.getQueryStatistics();
        context.getState().setEof();
        plannerProfile.setQueryFetchResultFinishTime();
    }

    private TransactionStatus getWaitingTxnStatus(TWaitingTxnStatusRequest request) throws Exception {
        TransactionStatus txnStatus = null;
        if (Catalog.getCurrentCatalog().isMaster()) {
            txnStatus = Catalog.getCurrentGlobalTransactionMgr()
                    .getWaitingTxnStatus(request);
        } else {
            MasterTxnExecutor masterTxnExecutor = new MasterTxnExecutor(context);
            txnStatus = masterTxnExecutor.getWaitingTxnStatus(request);
        }
        return txnStatus;
    }

    private void handleTransactionStmt() throws Exception {
        // Every time set no send flag and clean all data in buffer
        context.getMysqlChannel().reset();
        context.getState().setOk(0, 0, "");
        // create plan
        if (context.getTxnEntry() != null && context.getTxnEntry().getRowsInTransaction() == 0
                && (parsedStmt instanceof TransactionCommitStmt || parsedStmt instanceof TransactionRollbackStmt)) {
            context.setTxnEntry(null);
        } else if (parsedStmt instanceof TransactionBeginStmt) {
            if (context.isTxnModel()) {
                LOG.info("A transaction has already begin");
                return;
            }
            TTxnParams txnParams = new TTxnParams();
            txnParams.setNeedTxn(true).setThriftRpcTimeoutMs(5000).setTxnId(-1).setDb("").setTbl("");
            if (context.getTxnEntry() == null) {
                context.setTxnEntry(new TransactionEntry());
            }
            TransactionEntry txnEntry = context.getTxnEntry();
            txnEntry.setTxnConf(txnParams);
            StringBuilder sb = new StringBuilder();
            sb.append("{'label':'").append(context.getTxnEntry().getLabel()).append("', 'status':'")
                    .append(TransactionStatus.PREPARE.name());
            sb.append("', 'txnId':'").append("'").append("}");
            context.getState().setOk(0, 0, sb.toString());
        } else if (parsedStmt instanceof TransactionCommitStmt) {
            if (!context.isTxnModel()) {
                LOG.info("No transaction to commit");
                return;
            }

            TTxnParams txnConf = context.getTxnEntry().getTxnConf();
            PUniqueId fragmentInstanceId = new PUniqueId();
            fragmentInstanceId.hi = txnConf.getFragmentInstanceId().getHi();
            fragmentInstanceId.lo = txnConf.getFragmentInstanceId().getLo();

            try {
                if (context.getTxnEntry().getDataToSend().size() > 0) {
                    BackendServiceProxy.getInstance().insertForTxn(
                            fragmentInstanceId, context.getTxnEntry().getDataToSend(), context.getTxnEntry().getBackend());
                    context.getTxnEntry().clearDataToSend();
                }

                BackendServiceProxy.getInstance().commitForTxn(fragmentInstanceId, context.getTxnEntry().getBackend());
                TWaitingTxnStatusRequest request = new TWaitingTxnStatusRequest();
                request.setDbId(txnConf.getDbId()).setTxnId(txnConf.getTxnId());
                request.setLabelIsSet(false);
                request.setTxnIdIsSet(true);
                TransactionStatus txnStatus = getWaitingTxnStatus(request);
                if (txnStatus == TransactionStatus.COMMITTED) {
                    throw new AnalysisException("transaction commit successfully, BUT data will be visible later.");
                } else if (txnStatus != TransactionStatus.VISIBLE) {
                    throw new AnalysisException("commit failed, rollback.");
                }
                StringBuilder sb = new StringBuilder();
                sb.append("{'label':'").append(context.getTxnEntry().getLabel()).append("', 'status':'")
                        .append(txnStatus.name()).append("', 'txnId':'")
                        .append(context.getTxnEntry().getTxnConf().getTxnId()).append("'").append("}");
                context.getState().setOk(0, 0, sb.toString());
            } catch (Exception e) {
                throw new AnalysisException(e.getMessage());
            } finally {
                context.setTxnEntry(null);
            }
        } else if (parsedStmt instanceof TransactionRollbackStmt) {
            if (!context.isTxnModel()) {
                LOG.info("No transaction to rollback");
                return;
            }
            TTxnParams txnConf = context.getTxnEntry().getTxnConf();
            PUniqueId fragmentInstanceId = new PUniqueId();
            fragmentInstanceId.hi = txnConf.getFragmentInstanceId().getHi();
            fragmentInstanceId.lo = txnConf.getFragmentInstanceId().getLo();

            try {
                BackendServiceProxy.getInstance().rollbackForTxn(
                        fragmentInstanceId, context.getTxnEntry().getBackend());
                StringBuilder sb = new StringBuilder();
                sb.append("{'label':'").append(context.getTxnEntry().getLabel()).append("', 'status':'")
                        .append(TransactionStatus.ABORTED.name()).append("', 'txnId':'")
                        .append(context.getTxnEntry().getTxnConf().getTxnId()).append("'").append("}");
                context.getState().setOk(0, 0, sb.toString());
            } catch (Exception e) {
                throw new AnalysisException(e.getMessage());
            } finally {
                context.setTxnEntry(null);
            }
        } else {
            throw new TException("parsedStmt type is not TransactionStmt");
        }
    }

    public int executeForTxn(InsertStmt insertStmt)
            throws UserException, TException, InterruptedException, ExecutionException, TimeoutException {
        if (context.isTxnIniting()) { // first time, begin txn
            beginTxn(insertStmt.getDb(), insertStmt.getTbl());
        }
        if (!context.getTxnEntry().getTxnConf().getDb().equals(insertStmt.getDb()) ||
                !context.getTxnEntry().getTxnConf().getTbl().equals(insertStmt.getTbl())) {
            throw new TException("Only one table can be inserted in one transaction.");
        }
        PUniqueId fragmentInstanceId = new PUniqueId();
        fragmentInstanceId.hi = context.getTxnEntry().getTxnConf().getFragmentInstanceId().getHi();
        fragmentInstanceId.lo = context.getTxnEntry().getTxnConf().getFragmentInstanceId().getLo();

        QueryStmt queryStmt = insertStmt.getQueryStmt();
        if (!(queryStmt instanceof SelectStmt)) {
            throw new TException("queryStmt is not SelectStmt, insert command error");
        }
        SelectStmt selectStmt = (SelectStmt) queryStmt;
        int effectRows = 0;
        if (selectStmt.getValueList() != null) {
            Table tbl = context.getTxnEntry().getTable();
            for (List<Expr> row : selectStmt.getValueList().getRows()) {
                if (tbl.getFullSchema().size() != row.size()) {
                    throw new TException("Column count doesn't match value count");
                }
            }
            for (List<Expr> row : selectStmt.getValueList().getRows()) {
                ++effectRows;
                String data = getRowStringValue(row);
                List<String> dataToSend = context.getTxnEntry().getDataToSend();
                dataToSend.add(data);
                if (dataToSend.size() >= MAX_DATA_TO_SEND_FOR_TXN) {
                    BackendServiceProxy.getInstance().insertForTxn(
                            fragmentInstanceId, dataToSend, context.getTxnEntry().getBackend());
                    context.getTxnEntry().clearDataToSend();
                }
            }
        }
        context.getTxnEntry().setRowsInTransaction(context.getTxnEntry().getRowsInTransaction() + effectRows);
        return effectRows;
    }
    private void beginTxn(String dbName, String tblName) throws UserException, TException,
            InterruptedException, ExecutionException, TimeoutException {
        TransactionEntry txnEntry = context.getTxnEntry();
        TTxnParams txnConf = txnEntry.getTxnConf();
        long timeoutSecond = ConnectContext.get().getSessionVariable().getQueryTimeoutS();
        TransactionState.LoadJobSourceType sourceType = TransactionState.LoadJobSourceType.INSERT_STREAMING;
        Database dbObj = Catalog.getCurrentCatalog().getDb(dbName);
        if (dbObj == null) {
            throw new TException("database is invalid for dbName: " + dbName);
        }
        Table tblObj = dbObj.getTable(tblName);
        if (tblObj == null) {
            throw new TException("table is invalid: " + tblName);
        }
        txnEntry.setTable(tblObj);
        txnConf.setDbId(dbObj.getId()).setTbl(tblName).setDb(dbName);
        String label = txnEntry.getLabel();
        if (Catalog.getCurrentCatalog().isMaster()) {
            long txnId = Catalog.getCurrentGlobalTransactionMgr().beginTransaction(
                    txnConf.getDbId(), Lists.newArrayList(tblObj.getId()),
                    label, new TransactionState.TxnCoordinator(
                            TransactionState.TxnSourceType.FE, FrontendOptions.getLocalHostAddress()),
                    sourceType, timeoutSecond);
            txnConf.setTxnId(txnId);
            String authCodeUuid = Catalog.getCurrentGlobalTransactionMgr().getTransactionState(
                    txnConf.getDbId(), txnConf.getTxnId()).getAuthCode();
            txnConf.setAuthCodeUuid(authCodeUuid);
        } else {
            String authCodeUuid = UUID.randomUUID().toString();
            MasterTxnExecutor masterTxnExecutor = new MasterTxnExecutor(context);
            TLoadTxnBeginRequest request = new TLoadTxnBeginRequest();
            request.setDb(txnConf.getDb()).setTbl(txnConf.getTbl()).setAuthCodeUuid(authCodeUuid)
                    .setCluster(dbObj.getClusterName()).setLabel(label).setUser("").setUserIp("").setPasswd("");
            TLoadTxnBeginResult result = masterTxnExecutor.beginTxn(request);
            txnConf.setTxnId(result.getTxnId());
            txnConf.setAuthCodeUuid(authCodeUuid);
        }

        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        request.setTxnId(txnConf.getTxnId()).setDb(txnConf.getDb())
                .setTbl(txnConf.getTbl())
                .setFileType(TFileType.FILE_STREAM).setFormatType(TFileFormatType.FORMAT_CSV_PLAIN)
                .setMergeType(TMergeType.APPEND).setThriftRpcTimeoutMs(5000).setLoadId(context.queryId());

        StreamLoadTask streamLoadTask = StreamLoadTask.fromTStreamLoadPutRequest(request);
        StreamLoadPlanner planner = new StreamLoadPlanner(dbObj,
                (OlapTable) tblObj, streamLoadTask);
        TExecPlanFragmentParams tRequest = planner.plan(streamLoadTask.getId());

        List<Long> beIds = Catalog.getCurrentSystemInfo().seqChooseBackendIds(
                1, true, true, dbObj.getClusterName());
        if (beIds == null) {
            throw new TException("no backend, you can't insert data");
        }
        Backend backend = Catalog.getCurrentSystemInfo().getIdToBackend().get(beIds.get(0));
        txnConf.setFragmentInstanceId(tRequest.params.fragment_instance_id);
        txnConf.setUserIp(backend.getHost());
        tRequest.setTxnConf(txnConf).setImportLabel(label);
        txnEntry.setBackend(backend);
        PExecPlanFragmentResult result = execRemoteFragmentAsync(backend, tRequest)
                .get(Config.remote_fragment_exec_timeout_ms, TimeUnit.MILLISECONDS);
        if (result.status.error_msgs != null && !result.status.error_msgs.isEmpty()) {
            throw new TException(result.status.error_msgs.get(0));
        }
    }

    public Future<PExecPlanFragmentResult> execRemoteFragmentAsync(
            Backend backend, TExecPlanFragmentParams rpcParams) throws TException {
        TNetworkAddress brpcAddress = null;
        try {
            brpcAddress = new TNetworkAddress(backend.getHost(), backend.getBrpcPort());
        } catch (Exception e) {
            throw new TException(e.getMessage());
        }
        try {
            return BackendServiceProxy.getInstance().execPlanFragmentAsync(brpcAddress, rpcParams);
        } catch (RpcException e) {
            // DO NOT throw exception here, return a complete future with error code,
            // so that the following logic will cancel the fragment.
            return new Future<PExecPlanFragmentResult>() {
                @Override
                public boolean cancel(boolean mayInterruptIfRunning) {
                    return false;
                }

                @Override
                public boolean isCancelled() {
                    return false;
                }

                @Override
                public boolean isDone() {
                    return true;
                }

                @Override
                public PExecPlanFragmentResult get() {
                    PExecPlanFragmentResult result = new PExecPlanFragmentResult();
                    PStatus pStatus = new PStatus();
                    pStatus.error_msgs = Lists.newArrayList();
                    pStatus.error_msgs.add(e.getMessage());
                    // use THRIFT_RPC_ERROR so that this BE will be added to the blacklist later.
                    pStatus.status_code = TStatusCode.THRIFT_RPC_ERROR.getValue();
                    result.status = pStatus;
                    return result;
                }

                @Override
                public PExecPlanFragmentResult get(long timeout, TimeUnit unit) {
                    return get();
                }
            };
        }
    }

    public static String getRowStringValue(List<Expr> cols) {
        if (cols.size() == 0) {
            return "";
        }
        List<String> vals = new ArrayList<>();
        for (Expr expr : cols) {
            vals.add(expr.getStringValue());
        }
        return String.join("\t", vals);
    }

    // Process a select statement.
    private void handleInsertStmt() throws Exception {
        // Every time set no send flag and clean all data in buffer
        context.getMysqlChannel().reset();
        // create plan
        InsertStmt insertStmt = null;
        if (parsedStmt instanceof CreateTableAsSelectStmt) {
            // Create table here
            ((CreateTableAsSelectStmt) parsedStmt).createTable(analyzer);
            insertStmt = ((CreateTableAsSelectStmt) parsedStmt).getInsertStmt();
        } else {
            insertStmt = (InsertStmt) parsedStmt;
        }

        if (insertStmt.getQueryStmt().hasOutFileClause()) {
            throw new DdlException("Not support OUTFILE clause in INSERT statement");
        }

        if (insertStmt.getQueryStmt().isExplain()) {
            String explainString = planner.getExplainString(planner.getFragments(), TExplainLevel.VERBOSE);
            handleExplainStmt(explainString);
            return;
        }

        long createTime = System.currentTimeMillis();
        Throwable throwable = null;
        long txnId = -1;
        String label = "";
        long loadedRows = 0;
        int filteredRows = 0;
        TransactionStatus txnStatus = TransactionStatus.ABORTED;
        String errMsg = "";
        if (context.isTxnModel()) {
            if (insertStmt.getQueryStmt() instanceof SelectStmt) {
                if (((SelectStmt) insertStmt.getQueryStmt()).getTableRefs().size() > 0) {
                    throw new TException("Insert into ** select is not supported in a transaction");
                }
            }
            txnStatus = TransactionStatus.PREPARE;
            loadedRows = executeForTxn(insertStmt);
            label = context.getTxnEntry().getLabel();
            txnId = context.getTxnEntry().getTxnConf().getTxnId();
        } else {
            label = insertStmt.getLabel();
            LOG.info("Do insert [{}] with query id: {}", label, DebugUtil.printId(context.queryId()));
            try {
                coord = new Coordinator(context, analyzer, planner);
                coord.setQueryType(TQueryType.LOAD);

                QeProcessorImpl.INSTANCE.registerQuery(context.queryId(), coord);

                coord.exec();

                coord.join(context.getSessionVariable().getQueryTimeoutS());
                if (!coord.isDone()) {
                    coord.cancel();
                    ErrorReport.reportDdlException(ErrorCode.ERR_EXECUTE_TIMEOUT);
                }

                if (!coord.getExecStatus().ok()) {
                    errMsg = coord.getExecStatus().getErrorMsg();
                    LOG.warn("insert failed: {}", errMsg);
                    ErrorReport.reportDdlException(errMsg, ErrorCode.ERR_FAILED_WHEN_INSERT);
                }

                LOG.debug("delta files is {}", coord.getDeltaUrls());

                if (coord.getLoadCounters().get(LoadEtlTask.DPP_NORMAL_ALL) != null) {
                    loadedRows = Long.valueOf(coord.getLoadCounters().get(LoadEtlTask.DPP_NORMAL_ALL));
                }
                if (coord.getLoadCounters().get(LoadEtlTask.DPP_ABNORMAL_ALL) != null) {
                    filteredRows = Integer.valueOf(coord.getLoadCounters().get(LoadEtlTask.DPP_ABNORMAL_ALL));
                }

                // if in strict mode, insert will fail if there are filtered rows
                if (context.getSessionVariable().getEnableInsertStrict()) {
                    if (filteredRows > 0) {
                        context.getState().setError("Insert has filtered data in strict mode, tracking_url="
                                + coord.getTrackingUrl());
                        return;
                    }
                }

                if (insertStmt.getTargetTable().getType() != TableType.OLAP) {
                    // no need to add load job.
                    // MySQL table is already being inserted.
                    context.getState().setOk(loadedRows, filteredRows, null);
                    return;
                }

                if (loadedRows == 0 && filteredRows == 0) {
                    // if no data, just abort txn and return ok
                    Catalog.getCurrentGlobalTransactionMgr().abortTransaction(insertStmt.getDbObj().getId(),
                            insertStmt.getTransactionId(), TransactionCommitFailedException.NO_DATA_TO_LOAD_MSG);
                    context.getState().setOk();
                    return;
                }
                if (Catalog.getCurrentGlobalTransactionMgr().commitAndPublishTransaction(
                        insertStmt.getDbObj(), Lists.newArrayList(insertStmt.getTargetTable()), insertStmt.getTransactionId(),
                        TabletCommitInfo.fromThrift(coord.getCommitInfos()),
                        context.getSessionVariable().getInsertVisibleTimeoutMs())) {
                    txnStatus = TransactionStatus.VISIBLE;
                    MetricRepo.COUNTER_LOAD_FINISHED.increase(1L);
                } else {
                    txnStatus = TransactionStatus.COMMITTED;
                }

            } catch (Throwable t) {
                // if any throwable being thrown during insert operation, first we should abort this txn
                LOG.warn("handle insert stmt fail: {}", label, t);
                try {
                    Catalog.getCurrentGlobalTransactionMgr().abortTransaction(
                            insertStmt.getDbObj().getId(), insertStmt.getTransactionId(),
                            t.getMessage() == null ? "unknown reason" : t.getMessage());
                } catch (Exception abortTxnException) {
                    // just print a log if abort txn failed. This failure do not need to pass to user.
                    // user only concern abort how txn failed.
                    LOG.warn("errors when abort txn", abortTxnException);
                }

                if (!Config.using_old_load_usage_pattern) {
                    // if not using old load usage pattern, error will be returned directly to user
                    StringBuilder sb = new StringBuilder(t.getMessage());
                    if (!Strings.isNullOrEmpty(coord.getTrackingUrl())) {
                        sb.append(". url: " + coord.getTrackingUrl());
                    }
                    context.getState().setError(sb.toString());
                    return;
                }

                /*
                 * If config 'using_old_load_usage_pattern' is true.
                 * Doris will return a label to user, and user can use this label to check load job's status,
                 * which exactly like the old insert stmt usage pattern.
                 */
                throwable = t;
            }

            // Go here, which means:
            // 1. transaction is finished successfully (COMMITTED or VISIBLE), or
            // 2. transaction failed but Config.using_old_load_usage_pattern is true.
            // we will record the load job info for these 2 cases

            try {
                context.getCatalog().getLoadManager().recordFinishedLoadJob(
                        label,
                        insertStmt.getDb(),
                        insertStmt.getTargetTable().getId(),
                        EtlJobType.INSERT,
                        createTime,
                        throwable == null ? "" : throwable.getMessage(),
                        coord.getTrackingUrl());
            } catch (MetaNotFoundException e) {
                LOG.warn("Record info of insert load with error {}", e.getMessage(), e);
                errMsg = "Record info of insert load with error " + e.getMessage();
            }
            txnId = insertStmt.getTransactionId();
        }

        // {'label':'my_label1', 'status':'visible', 'txnId':'123'}
        // {'label':'my_label1', 'status':'visible', 'txnId':'123' 'err':'error messages'}
        StringBuilder sb = new StringBuilder();
        sb.append("{'label':'").append(label).append("', 'status':'").append(txnStatus.name());
        sb.append("', 'txnId':'").append(txnId).append("'");
        if (!Strings.isNullOrEmpty(errMsg)) {
            sb.append(", 'err':'").append(errMsg).append("'");
        }
        sb.append("}");

        context.getState().setOk(loadedRows, filteredRows, sb.toString());
    }

    private void handleUnsupportedStmt() {
        context.getMysqlChannel().reset();
        // do nothing
        context.getState().setOk();
    }

    // Process use statement.
    private void handleUseStmt() throws AnalysisException {
        UseStmt useStmt = (UseStmt) parsedStmt;
        try {
            if (Strings.isNullOrEmpty(useStmt.getClusterName())) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_CLUSTER_NO_SELECT_CLUSTER);
            }
            context.getCatalog().changeDb(context, useStmt.getDatabase());
        } catch (DdlException e) {
            context.getState().setError(e.getMessage());
            return;
        }
        context.getState().setOk();
    }

    private void sendMetaData(ShowResultSetMetaData metaData) throws IOException {
        // sends how many columns
        serializer.reset();
        serializer.writeVInt(metaData.getColumnCount());
        context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
        // send field one by one
        for (Column col : metaData.getColumns()) {
            serializer.reset();
            // TODO(zhaochun): only support varchar type
            serializer.writeField(col.getName(), col.getType().getPrimitiveType());
            context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
        }
        // send EOF
        serializer.reset();
        MysqlEofPacket eofPacket = new MysqlEofPacket(context.getState());
        eofPacket.writeTo(serializer);
        context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
    }

    private void sendFields(List<String> colNames, List<PrimitiveType> types) throws IOException {
        // sends how many columns
        serializer.reset();
        serializer.writeVInt(colNames.size());
        context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
        // send field one by one
        for (int i = 0; i < colNames.size(); ++i) {
            serializer.reset();
            serializer.writeField(colNames.get(i), types.get(i));
            context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
        }
        // send EOF
        serializer.reset();
        MysqlEofPacket eofPacket = new MysqlEofPacket(context.getState());
        eofPacket.writeTo(serializer);
        context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
    }

    public void sendShowResult(ShowResultSet resultSet) throws IOException {
        context.updateReturnRows(resultSet.getResultRows().size());
        // Send meta data.
        sendMetaData(resultSet.getMetaData());

        // Send result set.
        for (List<String> row : resultSet.getResultRows()) {
            serializer.reset();
            for (String item : row) {
                if (item == null || item.equals(FeConstants.null_string)) {
                    serializer.writeNull();
                } else {
                    serializer.writeLenEncodedString(item);
                }
            }
            context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
        }

        context.getState().setEof();
    }
    // Process show statement
    private void handleShow() throws IOException, AnalysisException, DdlException {
        ShowExecutor executor = new ShowExecutor(context, (ShowStmt) parsedStmt);
        ShowResultSet resultSet = executor.execute();
        if (resultSet == null) {
            // state changed in execute
            return;
        }
        if (isProxy) {
            proxyResultSet = resultSet;
            return;
        }

        sendShowResult(resultSet);
    }

    private void handleExplainStmt(String result) throws IOException {
        ShowResultSetMetaData metaData =
                ShowResultSetMetaData.builder()
                        .addColumn(new Column("Explain String", ScalarType.createVarchar(20)))
                        .build();
        sendMetaData(metaData);

        // Send result set.
        for (String item : result.split("\n")) {
            serializer.reset();
            serializer.writeLenEncodedString(item);
            context.getMysqlChannel().sendOnePacket(serializer.toByteBuffer());
        }
        context.getState().setEof();
    }

    private void handleDdlStmt() {
        try {
            DdlExecutor.execute(context.getCatalog(), (DdlStmt) parsedStmt);
            context.getState().setOk();
        } catch (QueryStateException e) {
            context.setState(e.getQueryState());
        } catch (UserException e) {
            // Return message to info client what happened.
            context.getState().setError(e.getMessage());
        } catch (Exception e) {
            // Maybe our bug
            LOG.warn("DDL statement(" + originStmt.originStmt + ") process failed.", e);
            context.getState().setError("Unexpected exception: " + e.getMessage());
        }
    }

    // process enter cluster
    private void handleEnterStmt() {
        final EnterStmt enterStmt = (EnterStmt) parsedStmt;
        try {
            context.getCatalog().changeCluster(context, enterStmt.getClusterName());
            context.setDatabase("");
        } catch (DdlException e) {
            context.getState().setError(e.getMessage());
            return;
        }
        context.getState().setOk();
    }

    private void handleExportStmt() throws Exception {
        ExportStmt exportStmt = (ExportStmt) parsedStmt;
        context.getCatalog().getExportMgr().addExportJob(exportStmt);
    }

    public PQueryStatistics getQueryStatisticsForAuditLog() {
        if (statisticsForAuditLog == null) {
            statisticsForAuditLog = new PQueryStatistics();
        }
        if (statisticsForAuditLog.scan_bytes == null) {
            statisticsForAuditLog.scan_bytes = 0L;
        }
        if (statisticsForAuditLog.scan_rows == null) {
            statisticsForAuditLog.scan_rows = 0L;
        }
        if (statisticsForAuditLog.returned_rows == null) {
            statisticsForAuditLog.returned_rows = 0L;
        }
        if (statisticsForAuditLog.cpu_ms == null) {
            statisticsForAuditLog.cpu_ms = 0L;
        }
        return statisticsForAuditLog;
    }

    private List<PrimitiveType> exprToType(List<Expr> exprs) {
        return exprs.stream().map(e -> e.getType().getPrimitiveType()).collect(Collectors.toList());
    }
}

