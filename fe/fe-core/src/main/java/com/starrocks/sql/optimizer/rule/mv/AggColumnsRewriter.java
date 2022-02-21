// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.sql.optimizer.rule.mv;

import com.starrocks.catalog.Column;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptExpressionVisitor;
import com.starrocks.sql.optimizer.operator.logical.LogicalAggregationOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AggColumnsRewriter extends OptExpressionVisitor<OptExpression, MaterializedViewRule> {

    LogicalOlapScanOperator scanOperator;

    public AggColumnsRewriter(LogicalOlapScanOperator scanOperator) {
        this.scanOperator = scanOperator;
    }

    public OptExpression rewrite(OptExpression optExpression, MaterializedViewRule context) {
        return optExpression.getOp().accept(this, optExpression, context);
    }

    @Override
    public OptExpression visit(OptExpression optExpression, MaterializedViewRule context) {
        for (int childIdx = 0; childIdx < optExpression.arity(); ++childIdx) {
            optExpression.setChild(childIdx, rewrite(optExpression.inputAt(childIdx), context));
        }

        return OptExpression.create(optExpression.getOp(), optExpression.getInputs());
    }

    @Override
    public OptExpression visitLogicalTableScan(OptExpression optExpression, MaterializedViewRule context) {
        LogicalOlapScanOperator olapScanOperator = (LogicalOlapScanOperator) optExpression.getOp();

        // Output columns need to be rewrite
        Map<ColumnRefOperator, Column> columnRefOperatorColumnMap =
                new HashMap<>(olapScanOperator.getColRefToColumnMetaMap());
        // Columns need to be removed from columnRefOperatorColumnMap after rewriting
        Set<ColumnRefOperator> removeSetForRefOperator = new HashSet<>();
        // <ColumnRefOperator, Column>
        for (Map.Entry<Integer, Set<CallOperator>> entry : context.getAggFunctions().entrySet()) {
            // Set<CallOperator>
            for (CallOperator callOperator : entry.getValue()) {
                List<ScalarOperator> arguments = callOperator.getChildren();
                if (arguments.isEmpty() || !(arguments.get(0) instanceof ColumnRefOperator)) {
                    continue;
                }
                ColumnRefOperator operator = (ColumnRefOperator) arguments.get(0);
                for (ColumnRefOperator column : columnRefOperatorColumnMap.keySet()) {
                    if (column.getId() == operator.getId()) {
                        // rewrite the column object with aggFnName
                        ColumnRefOperator newColumnRefOperator = context.getFactory().create(
                                column.getName(), column.getType(), column.isNullable()
                        );
                        newColumnRefOperator.setAggFnName(callOperator.getFnName());
                        columnRefOperatorColumnMap.put(newColumnRefOperator, columnRefOperatorColumnMap.get(column));
                        removeSetForRefOperator.add(column);
                        // Set rewrite columns mapping
                        context.addColumnRewriteItem(column, newColumnRefOperator);
                        break;
                    }
                }
            }
        }

        for (ColumnRefOperator refOperator : removeSetForRefOperator) {
            columnRefOperatorColumnMap.remove(refOperator);
        }

        LogicalOlapScanOperator newScanOperator = new LogicalOlapScanOperator(
                olapScanOperator.getTable(),
                columnRefOperatorColumnMap,
                olapScanOperator.getColumnMetaToColRefMap(),
                olapScanOperator.getDistributionSpec(),
                olapScanOperator.getLimit(),
                olapScanOperator.getPredicate(),
                olapScanOperator.getSelectedIndexId(),
                olapScanOperator.getSelectedPartitionId(),
                olapScanOperator.getPartitionNames(),
                olapScanOperator.getSelectedTabletId(),
                olapScanOperator.getHintsTabletIds());

        optExpression = OptExpression.create(newScanOperator);
        return optExpression;
    }

    @Override
    public OptExpression visitLogicalProject(OptExpression optExpression, MaterializedViewRule context) {
        for (int childIdx = 0; childIdx < optExpression.arity(); ++childIdx) {
            optExpression.setChild(childIdx, rewrite(optExpression.inputAt(childIdx), context));
        }
        LogicalProjectOperator projectOperator = (LogicalProjectOperator) optExpression.getOp();
        // rewrite columnRefMap of LogicalProjectOperator
        // 3:height -> 6:max(height) + 7:sum(height)
        Map<ColumnRefOperator, ScalarOperator> columnRefMap = projectOperator.getColumnRefMap();
        Map<ColumnRefOperator, ScalarOperator> newColumnRefMap = new HashMap<>(columnRefMap);
        Set<ColumnRefOperator> needRemoved = new HashSet<>();
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entrySet : columnRefMap.entrySet()) {
            ColumnRefOperator from = entrySet.getKey();
            LinkedHashSet<ColumnRefOperator> rewriteMapper = context.getColumnRewriteItem(from);
            if (rewriteMapper == null || rewriteMapper.isEmpty()) {
                newColumnRefMap.put(entrySet.getKey(), entrySet.getValue());
                continue;
            }
            for (ColumnRefOperator to : rewriteMapper) {
                newColumnRefMap.put(to, to);  // TODO: value is right?
                needRemoved.add(from);
            }
        }
        for (ColumnRefOperator removedOperator : needRemoved) {
            newColumnRefMap.remove(removedOperator);
        }
        LogicalProjectOperator newProjectOperator = new LogicalProjectOperator(
                newColumnRefMap, projectOperator.getLimit()
        );
        return OptExpression.create(newProjectOperator, optExpression.getInputs());
    }

    @Override
    public OptExpression visitLogicalAggregate(OptExpression optExpression, MaterializedViewRule context) {
        for (int childIdx = 0; childIdx < optExpression.arity(); ++childIdx) {
            optExpression.setChild(childIdx, rewrite(optExpression.inputAt(childIdx), context));
        }

        LogicalAggregationOperator aggregationOperator = (LogicalAggregationOperator) optExpression.getOp();
        Map<ColumnRefOperator, CallOperator> newAggMap = new HashMap<>(aggregationOperator.getAggregations());
        for (Map.Entry<ColumnRefOperator, CallOperator> kv : aggregationOperator.getAggregations().entrySet()) {
            List<ScalarOperator> arguments = kv.getValue().getChildren();
            List<ScalarOperator> newArguments = new ArrayList<>();
            for (int index = 0; index < arguments.size(); index++) {
                ScalarOperator op = arguments.get(index);
                if (op instanceof ColumnRefOperator) {
                    ColumnRefOperator from = (ColumnRefOperator) op;
                    LinkedHashSet<ColumnRefOperator> rewriteMapper = context.getColumnRewriteItem(from);
                    if (rewriteMapper == null || rewriteMapper.isEmpty()) {
                        newArguments.add(op);
                    } else {
                        for (ColumnRefOperator to : rewriteMapper) {
                            if (to.getAggFnName().equals(kv.getValue().getFnName())) {
                                kv.getValue().setChild(index, to);
                                break;
                            }
                        }
                    }
                } else {
                    newArguments.add(op);
                }
            }
        }
        return OptExpression.create(aggregationOperator, optExpression.getInputs());
    }
}
