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

package org.apache.doris.planner;

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.SelectStmt;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TableRef;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class MaterializedViewSelector {
    private static final Logger LOG = LogManager.getLogger(MaterializedViewSelector.class);

    private final SelectStmt selectStmt;
    private final Analyzer analyzer;

    private Map<String, Set<String>> columnNamesInPredicates = Maps.newHashMap();
    private boolean isSPJQuery;
    private Map<String, Set<String>> columnNamesInGrouping = Maps.newHashMap();
    private Map<String, Set<AggregatedColumn>> aggregateColumnsInQuery = Maps.newHashMap();
    private Map<String, Set<String>> columnNamesInQueryOutput = Maps.newHashMap();

    private boolean disableSPJGView;
    private String reasonOfDisable;
    private boolean isPreAggregation = true;

    public MaterializedViewSelector(SelectStmt selectStmt, Analyzer analyzer) {
        this.selectStmt = selectStmt;
        this.analyzer = analyzer;
        init();
    }

    /**
     * There are two stages to choosing the best MV.
     * Phase 1: Predicates
     * According to aggregation and column information in the select stmt,
     * the candidate MVs that meets the query conditions are selected.
     * Phase 2: Priorities
     * According to prefix index and row count in candidate MVs,
     * the best MV is selected.
     *
     * @param scanNode
     * @return
     */
    public void selectBestMV(ScanNode scanNode) throws UserException {
        long start = System.currentTimeMillis();
        Preconditions.checkState(scanNode instanceof OlapScanNode);
        OlapScanNode olapScanNode = (OlapScanNode) scanNode;
        Map<Long, List<Column>> candidateIndexIdToSchema = predicates(olapScanNode);
        long bestIndexId = priorities(olapScanNode, candidateIndexIdToSchema);
        LOG.info("The best materialized view is {} for scan node {} in query {}, cost {}",
                 bestIndexId, scanNode.getId(), selectStmt.toSql(), (System.currentTimeMillis() - start));
        olapScanNode.updateScanRangeInfo(bestIndexId, isPreAggregation, reasonOfDisable);
    }

    private Map<Long, List<Column>> predicates(OlapScanNode scanNode) {
        // Step1: all of predicates is compensating predicates
        Map<Long, List<Column>> candidateIndexIdToSchema = scanNode.getOlapTable().getVisibleIndexes();
        OlapTable table = scanNode.getOlapTable();
        Preconditions.checkState(table != null);
        String tableName = table.getName();
        // Step2: check all columns in compensating predicates are available in the view output
        checkCompensatingPredicates(columnNamesInPredicates.get(tableName), candidateIndexIdToSchema);
        // Step3: group by list in query is the subset of group by list in view or view contains no aggregation
        checkGrouping(columnNamesInGrouping.get(tableName), candidateIndexIdToSchema);
        // Step4: aggregation functions are available in the view output
        checkAggregationFunction(aggregateColumnsInQuery.get(tableName), candidateIndexIdToSchema);
        // Step5: columns required to compute output expr are available in the view output
        checkOutputColumns(columnNamesInQueryOutput.get(tableName), candidateIndexIdToSchema);
        // Step6: if table type is aggregate and the candidateIndexIdToSchema is empty,
        if (table.getKeysType() == KeysType.AGG_KEYS && candidateIndexIdToSchema.size() == 0) {
            // the base index will be added in the candidateIndexIdToSchema.
            compensateIndex(candidateIndexIdToSchema, scanNode.getOlapTable().getVisibleIndexes(),
                            table.getSchemaByIndexId(table.getBaseIndexId()).size());
        }
        return candidateIndexIdToSchema;
    }

    private long priorities(OlapScanNode scanNode, Map<Long, List<Column>> candidateIndexIdToSchema) {
        // Step1: the candidate indexes that satisfies the most prefix index
        final Set<String> equivalenceColumns = Sets.newHashSet();
        final Set<String> unequivalenceColumns = Sets.newHashSet();
        scanNode.collectColumns(analyzer, equivalenceColumns, unequivalenceColumns);
        Set<Long> indexesMatchingBestPrefixIndex =
                matchBestPrefixIndex(candidateIndexIdToSchema, equivalenceColumns, unequivalenceColumns);
        if (indexesMatchingBestPrefixIndex.isEmpty()) {
            indexesMatchingBestPrefixIndex = candidateIndexIdToSchema.keySet();
        }

        // Step2: the best index that satisfies the least number of rows
        return selectBestRowCountIndex(indexesMatchingBestPrefixIndex, scanNode.getOlapTable(), scanNode
                .getSelectedPartitionIds());
    }

    private Set<Long> matchBestPrefixIndex(Map<Long, List<Column>> candidateIndexIdToSchema,
                                           Set<String> equivalenceColumns,
                                           Set<String> unequivalenceColumns) {
        if (equivalenceColumns.size() == 0 && unequivalenceColumns.size() == 0) {
            return candidateIndexIdToSchema.keySet();
        }
        Set<Long> indexesMatchingBestPrefixIndex = Sets.newHashSet();
        int maxPrefixMatchCount = 0;
        for (Map.Entry<Long, List<Column>> entry : candidateIndexIdToSchema.entrySet()) {
            int prefixMatchCount = 0;
            long indexId = entry.getKey();
            List<Column> indexSchema = entry.getValue();
            for (Column col : indexSchema) {
                if (equivalenceColumns.contains(col.getName())) {
                    prefixMatchCount++;
                } else if (unequivalenceColumns.contains(col.getName())) {
                    // Unequivalence predicate's columns can match only first column in rollup.
                    prefixMatchCount++;
                    break;
                } else {
                    break;
                }
            }

            if (prefixMatchCount == maxPrefixMatchCount) {
                LOG.debug("find a equal prefix match index {}. match count: {}", indexId, prefixMatchCount);
                indexesMatchingBestPrefixIndex.add(indexId);
            } else if (prefixMatchCount > maxPrefixMatchCount) {
                LOG.debug("find a better prefix match index {}. match count: {}", indexId, prefixMatchCount);
                maxPrefixMatchCount = prefixMatchCount;
                indexesMatchingBestPrefixIndex.clear();
                indexesMatchingBestPrefixIndex.add(indexId);
            }
        }
        LOG.debug("Those mv match the best prefix index:" + Joiner.on(",").join(indexesMatchingBestPrefixIndex));
        return indexesMatchingBestPrefixIndex;
    }

    private long selectBestRowCountIndex(Set<Long> indexesMatchingBestPrefixIndex, OlapTable olapTable,
                                         Collection<Long> partitionIds) {
        long minRowCount = Long.MAX_VALUE;
        long selectedIndexId = 0;
        for (Long indexId : indexesMatchingBestPrefixIndex) {
            long rowCount = 0;
            for (Long partitionId : partitionIds) {
                rowCount += olapTable.getPartition(partitionId).getIndex(indexId).getRowCount();
            }
            LOG.debug("rowCount={} for table={}", rowCount, indexId);
            if (rowCount < minRowCount) {
                minRowCount = rowCount;
                selectedIndexId = indexId;
            } else if (rowCount == minRowCount) {
                // check column number, select one minimum column number
                int selectedColumnSize = olapTable.getIndexIdToSchema().get(selectedIndexId).size();
                int currColumnSize = olapTable.getIndexIdToSchema().get(indexId).size();
                if (currColumnSize < selectedColumnSize) {
                    selectedIndexId = indexId;
                }
            }
        }
        String tableName = olapTable.getName();
        String v2RollupIndexName = "__v2_" + tableName;
        Long v2RollupIndex = olapTable.getIndexIdByName(v2RollupIndexName);
        long baseIndexId = olapTable.getBaseIndexId();
        ConnectContext connectContext = ConnectContext.get();
        boolean useV2Rollup = false;
        if (connectContext != null) {
            useV2Rollup = connectContext.getSessionVariable().getUseV2Rollup();
        }
        if (baseIndexId == selectedIndexId && v2RollupIndex != null && useV2Rollup) {
            // if the selectedIndexId is baseIndexId
            // check whether there is a V2 rollup index and useV2Rollup flag is true,
            // if both true, use v2 rollup index
            selectedIndexId = v2RollupIndex;
        }
        if (!useV2Rollup && v2RollupIndex != null && v2RollupIndex == selectedIndexId) {
            // if the selectedIndexId is v2RollupIndex
            // but useV2Rollup is false, use baseIndexId as selectedIndexId
            // just make sure to use baseIndex instead of v2RollupIndex if the useV2Rollup is false
            selectedIndexId = baseIndexId;
        }
        return selectedIndexId;
    }

    private void checkCompensatingPredicates(Set<String> columnsInPredicates,
                                             Map<Long, List<Column>> candidateIndexIdToSchema) {
        // When the query statement does not contain any columns in predicates, all candidate index can pass this check
        if (columnsInPredicates == null) {
            return;
        }
        Iterator<Map.Entry<Long, List<Column>>> iterator = candidateIndexIdToSchema.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, List<Column>> entry = iterator.next();
            Set<String> indexNonAggregatedColumnNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            entry.getValue().stream().filter(column -> !column.isAggregated())
                    .forEach(column -> indexNonAggregatedColumnNames.add(column.getName()));
            if (!indexNonAggregatedColumnNames.containsAll(columnsInPredicates)) {
                iterator.remove();
            }
        }
        LOG.debug("Those mv pass the test of compensating predicates:"
                          + Joiner.on(",").join(candidateIndexIdToSchema.keySet()));
    }

    /**
     * View      Query        result
     * SPJ       SPJG OR SPJ  pass
     * SPJG      SPJ          fail
     * SPJG      SPJG         pass
     * 1. grouping columns in query is subset of grouping columns in view
     * 2. the empty grouping columns in query is subset of all of views
     *
     * @param columnsInGrouping
     * @param candidateIndexIdToSchema
     */

    private void checkGrouping(Set<String> columnsInGrouping, Map<Long, List<Column>> candidateIndexIdToSchema) {
        Iterator<Map.Entry<Long, List<Column>>> iterator = candidateIndexIdToSchema.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, List<Column>> entry = iterator.next();
            Set<String> indexNonAggregatedColumnNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            List<Column> candidateIndexSchema = entry.getValue();
            candidateIndexSchema.stream().filter(column -> !column.isAggregated())
                    .forEach(column -> indexNonAggregatedColumnNames.add(column.getName()));
            // When the candidate index is SPJ type, it passes the verification directly
            if (indexNonAggregatedColumnNames.size() == candidateIndexSchema.size()) {
                continue;
            }
            // When the query is SPJ type but the candidate index is SPJG type, it will not pass directly.
            if (isSPJQuery || disableSPJGView) {
                iterator.remove();
                continue;
            }
            // The query is SPJG. The candidate index is SPJG too.
            // The grouping columns in query is empty. For example: select sum(A) from T
            if (columnsInGrouping == null) {
                continue;
            }
            // The grouping columns in query must be subset of the grouping columns in view
            if (!indexNonAggregatedColumnNames.containsAll(columnsInGrouping)) {
                iterator.remove();
            }
        }
        LOG.debug("Those mv pass the test of grouping:"
                          + Joiner.on(",").join(candidateIndexIdToSchema.keySet()));
    }

    private void checkAggregationFunction(Set<AggregatedColumn> aggregatedColumnsInQueryOutput,
                                          Map<Long, List<Column>> candidateIndexIdToSchema) {
        Iterator<Map.Entry<Long, List<Column>>> iterator = candidateIndexIdToSchema.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, List<Column>> entry = iterator.next();
            List<AggregatedColumn> indexAggregatedColumns = Lists.newArrayList();
            List<Column> candidateIndexSchema = entry.getValue();
            candidateIndexSchema.stream().filter(column -> column.isAggregated())
                    .forEach(column -> indexAggregatedColumns.add(
                            new AggregatedColumn(column.getName(), column.getAggregationType().name())));
            // When the candidate index is SPJ type, it passes the verification directly
            if (indexAggregatedColumns.size() == 0) {
                continue;
            }
            // When the query is SPJ type but the candidate index is SPJG type, it will not pass directly.
            if (isSPJQuery || disableSPJGView) {
                iterator.remove();
                continue;
            }
            // The query is SPJG. The candidate index is SPJG too.
            if (aggregatedColumnsInQueryOutput == null) {
                continue;
            }
            // The aggregated columns in query output must be subset of the aggregated columns in view
            if (!indexAggregatedColumns.containsAll(aggregatedColumnsInQueryOutput)) {
                iterator.remove();
            }
        }
        LOG.debug("Those mv pass the test of aggregation function:"
                          + Joiner.on(",").join(candidateIndexIdToSchema.keySet()));
    }

    private void checkOutputColumns(Set<String> columnNamesInQueryOutput,
                                    Map<Long, List<Column>> candidateIndexIdToSchema) {
        Iterator<Map.Entry<Long, List<Column>>> iterator = candidateIndexIdToSchema.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<Long, List<Column>> entry = iterator.next();
            Set<String> indexColumnNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
            List<Column> candidateIndexSchema = entry.getValue();
            candidateIndexSchema.stream().forEach(column -> indexColumnNames.add(column.getName()));
            // The aggregated columns in query output must be subset of the aggregated columns in view
            if (!indexColumnNames.containsAll(columnNamesInQueryOutput)) {
                iterator.remove();
            }
        }
        LOG.debug("Those mv pass the test of output columns:"
                          + Joiner.on(",").join(candidateIndexIdToSchema.keySet()));
    }

    private void compensateIndex(Map<Long, List<Column>> candidateIndexIdToSchema,
                                 Map<Long, List<Column>> allVisibleIndexes,
                                 int sizeOfBaseIndex) {
        isPreAggregation = false;
        reasonOfDisable = "The aggregate operator does not match";
        for (Map.Entry<Long, List<Column>> index : allVisibleIndexes.entrySet()) {
            if (index.getValue().size() == sizeOfBaseIndex) {
                candidateIndexIdToSchema.put(index.getKey(), index.getValue());
            }
        }
        LOG.debug("Those mv pass the test of output columns:"
                          + Joiner.on(",").join(candidateIndexIdToSchema.keySet()));
    }

    private void init() {
        // Step1: compute the columns in compensating predicates
        Expr whereClause = selectStmt.getWhereClause();
        if (whereClause != null) {
            whereClause.getTableNameToColumnNames(columnNamesInPredicates);
        }
        for (TableRef tableRef : selectStmt.getTableRefs()) {
            if (tableRef.getOnClause() == null) {
                continue;
            }
            tableRef.getOnClause().getTableNameToColumnNames(columnNamesInPredicates);
        }

        if (selectStmt.getAggInfo() == null) {
            isSPJQuery = true;
        } else {
            // Step2: compute the columns in group by expr
            if (selectStmt.getAggInfo().getGroupingExprs() != null) {
                List<Expr> groupingExprs = selectStmt.getAggInfo().getGroupingExprs();
                for (Expr expr : groupingExprs) {
                    expr.getTableNameToColumnNames(columnNamesInGrouping);
                }
            }
            // Step3: compute the aggregation function
            for (FunctionCallExpr aggExpr : selectStmt.getAggInfo().getAggregateExprs()) {
                // Only sum, min, max function could appear in materialized views.
                // The number of children in these functions is one.
                if (aggExpr.getChildren().size() != 1) {
                    reasonOfDisable = "aggExpr has more than one child";
                    disableSPJGView = true;
                    break;
                }
                Expr aggChild0 = aggExpr.getChild(0);
                if (aggChild0 instanceof SlotRef) {
                    SlotRef slotRef = (SlotRef) aggChild0;
                    Preconditions.checkState(slotRef.getColumnName() != null);
                    Table table = slotRef.getDesc().getParent().getTable();
                    if (table == null) {
                        continue;
                    }
                    addAggregatedColumn(slotRef.getColumnName(), aggExpr.getFnName().getFunction(),
                                        table.getName());
                } else if ((aggChild0 instanceof CastExpr) && (aggChild0.getChild(0) instanceof SlotRef)) {
                    SlotRef slotRef = (SlotRef) aggChild0.getChild(0);
                    Preconditions.checkState(slotRef.getColumnName() != null);
                    Table table = slotRef.getDesc().getParent().getTable();
                    if (table == null) {
                        continue;
                    }
                    addAggregatedColumn(slotRef.getColumnName(), aggExpr.getFnName().getFunction(),
                                        table.getName());
                } else {
                    reasonOfDisable = "aggExpr.getChild(0)[" + aggExpr.getChild(0).debugString()
                            + "] is not SlotRef or CastExpr|CaseExpr";
                    disableSPJGView = true;
                    break;
                }
                // TODO(ml): select rollup by case expr
            }
        }

        // Step4: compute the output column
        for (Expr resultExpr : selectStmt.getResultExprs()) {
            resultExpr.getTableNameToColumnNames(columnNamesInQueryOutput);
        }
    }

    private void addAggregatedColumn(String columnName, String functionName, String tableName) {
        AggregatedColumn newAggregatedColumn = new AggregatedColumn(columnName, functionName);
        Set<AggregatedColumn> aggregatedColumns = aggregateColumnsInQuery.get(tableName);
        if (aggregatedColumns == null) {
            aggregatedColumns = Sets.newHashSet();
            aggregateColumnsInQuery.put(tableName, aggregatedColumns);
        }
        aggregatedColumns.add(newAggregatedColumn);
    }

    class AggregatedColumn {
        private String columnName;
        private String aggFunctionName;

        public AggregatedColumn(String columnName, String aggFunctionName) {
            this.columnName = columnName;
            this.aggFunctionName = aggFunctionName;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == this) {
                return true;
            }
            if (!(obj instanceof AggregatedColumn)) {
                return false;
            }
            AggregatedColumn input = (AggregatedColumn) obj;
            return this.columnName.equalsIgnoreCase(input.columnName)
                    && this.aggFunctionName.equalsIgnoreCase(input.aggFunctionName);
        }
    }
}


