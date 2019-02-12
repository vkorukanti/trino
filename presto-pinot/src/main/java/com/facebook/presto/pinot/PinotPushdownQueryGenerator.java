/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.pinot;

import com.facebook.presto.spi.pipeline.AggregationPipelineNode;
import com.facebook.presto.spi.pipeline.AggregationPipelineNode.Aggregation;
import com.facebook.presto.spi.pipeline.AggregationPipelineNode.GroupByColumn;
import com.facebook.presto.spi.pipeline.AggregationPipelineNode.Node;
import com.facebook.presto.spi.pipeline.FilterPipelineNode;
import com.facebook.presto.spi.pipeline.PipelineNode;
import com.facebook.presto.spi.pipeline.ProjectPipelineNode;
import com.facebook.presto.spi.pipeline.PushdownExpression;
import com.facebook.presto.spi.pipeline.PushdownExpresssionVisitor;
import com.facebook.presto.spi.pipeline.PushdownFunction;
import com.facebook.presto.spi.pipeline.PushdownInExpression;
import com.facebook.presto.spi.pipeline.PushdownInputColumn;
import com.facebook.presto.spi.pipeline.PushdownLiteral;
import com.facebook.presto.spi.pipeline.PushdownLogicalBinaryExpression;
import com.facebook.presto.spi.pipeline.TablePipelineNode;
import com.facebook.presto.spi.pipeline.TableScanPipeline;
import com.facebook.presto.spi.pipeline.TableScanPipelineVisitor;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class PinotPushdownQueryGenerator
{
    private PinotPushdownQueryGenerator()
    {
    }

    static String generate(TableScanPipeline scanPipeline)
    {
        GeneratorContext context = null;
        PinotPipelineNodeVisitor visitor = new PinotPipelineNodeVisitor();
        for (PipelineNode node : scanPipeline.getPipelineNodes()) {
            context = node.accept(visitor, context);
        }

        return context.toQuery();
    }

    private static class PinotPushdownExpressionVisitor
            extends PushdownExpresssionVisitor<String, LinkedHashMap<String, String>>
    {
        @Override
        public String visitInputColumn(PushdownInputColumn inputColumn, LinkedHashMap<String, String> context)
        {
            // TODO: throw error if the input column doesn't exist
            return context.get(inputColumn.getName());
        }

        @Override
        public String visitFunction(PushdownFunction function, LinkedHashMap<String, String> context)
        {
            return function.getName() + "(" +
                    function.getInputs().stream().map(i -> i.accept(this, context)).collect(Collectors.joining(", ")) + ")";
        }

        @Override
        public String visitLogicalBinary(PushdownLogicalBinaryExpression comparision, LinkedHashMap<String, String> context)
        {
            return "( " + comparision.getLeft().accept(this, context) + " " + comparision.getOperator() + " " + comparision.getRight().accept(this, context) + " )";
        }

        @Override
        public String visitInExpression(PushdownInExpression in, LinkedHashMap<String, String> context)
        {
            return in.getValue().accept(this, context) + " IN " + "(" + in.getArguments().stream().map(a -> a.accept(this, context)).collect(Collectors.joining(", ")) + ")";
        }

        @Override
        public String visitLiteral(PushdownLiteral literal, LinkedHashMap<String, String> context)
        {
            return literal.toString();
        }
    }

    private static class PinotPipelineNodeVisitor
            extends TableScanPipelineVisitor<GeneratorContext, GeneratorContext>
    {
        @Override
        public GeneratorContext visitAggregationNode(AggregationPipelineNode aggregation, GeneratorContext context)
        {
            requireNonNull(context, "context is null");

            // If the existing context already contains group by columns, then we need to convert the whole context into a query. Add for support
            // for aggregation on top of aggregated data only if Pinot supports sub queries in PSQL.
            checkArgument(context.groupByColumns == null || context.groupByColumns.isEmpty(), "already contains group by columns");

            GeneratorContext newContext = new GeneratorContext();
            newContext.from = context.from;
            newContext.filter = context.filter;

            List<String> groupByColumns = new ArrayList<>();
            for (Node expr : aggregation.getNodes()) {
                switch (expr.getNodeType()) {
                    case GROUP_BY: {
                        GroupByColumn groupByColumn = (GroupByColumn) expr;
                        String groupByInputColumn = groupByColumn.getInputColumn();
                        groupByColumns.add(context.colMapping.get(groupByInputColumn));
                        newContext.colMapping.put(groupByColumn.getOutputColumn(), context.colMapping.get(groupByInputColumn));
                        break;
                    }
                    case AGGREGATE: {
                        Aggregation aggr = (Aggregation) expr;
                        String newColumn;
                        if (aggr.getInputs() == null || aggr.getInputs().isEmpty()) {
                            newColumn = aggr.getFunction() + "(*)";
                        }
                        else {
                            newColumn = aggr.getFunction() + "(" + aggr.getInputs().stream().map(i -> context.colMapping.get(i)).collect(Collectors.joining(",")) + ")";
                        }
                        newContext.colMapping.put(aggr.getOutputColumn(), newColumn);
                        break;
                    }
                    default:
                        throw new UnsupportedOperationException("unknown aggregation expression: " + expr.getNodeType());
                }
            }
            if (!groupByColumns.isEmpty()) {
                newContext.groupByColumns = groupByColumns;
            }

            return newContext;
        }

        @Override
        public GeneratorContext visitFilterNode(FilterPipelineNode filter, GeneratorContext context)
        {
            requireNonNull(context, "context is null");
            checkArgument(context.groupByColumns == null || context.groupByColumns.isEmpty(), "filter on top of aggregated data not supported");
            GeneratorContext newContext = new GeneratorContext();
            newContext.colMapping = context.colMapping;
            newContext.filter = filter.getPredicate().accept(new PinotPushdownExpressionVisitor(), context.colMapping);
            newContext.from = context.from;

            return newContext;
        }

        @Override
        public GeneratorContext visitProjectNode(ProjectPipelineNode project, GeneratorContext context)
        {
            requireNonNull(context, "context is null");
            checkArgument(context.groupByColumns == null || context.groupByColumns.isEmpty(), "project on top of aggregated data not supported");

            GeneratorContext newContext = new GeneratorContext();
            newContext.from = context.from;
            newContext.filter = context.filter;
            List<PushdownExpression> pushdownExpressions = project.getNodes();
            List<String> outputColumns = project.getOutputColumns();
            for (int fieldId = 0; fieldId < pushdownExpressions.size(); fieldId++) {
                PushdownExpression pushdownExpression = pushdownExpressions.get(fieldId);
                String output = outputColumns.get(fieldId);
                newContext.colMapping.put(output, pushdownExpression.accept(new PinotPushdownExpressionVisitor(), context.colMapping));
            }

            return newContext;
        }

        @Override
        public GeneratorContext visitTableNode(TablePipelineNode table, GeneratorContext context)
        {
            checkArgument(context == null, "Table scan node is expected to have no context as input");
            GeneratorContext newContext = new GeneratorContext();
            List<String> inputColumns = table.getInputColumns();
            List<String> outputColumns = table.getOutputColumns();
            for (int fieldId = 0; fieldId < outputColumns.size(); fieldId++) {
                newContext.colMapping.put(outputColumns.get(fieldId), inputColumns.get(fieldId));
            }

            // In Pinot table name is exposed as schema also. Querying just the table is enough
            newContext.from = table.getTableName();

            return newContext;
        }
    }

    public static class GeneratorContext
    {
        LinkedHashMap<String, String> colMapping = new LinkedHashMap<>();
        String from;
        String filter;
        List<String> groupByColumns;
        String limit; // TODO: later
        String orderBy; // TODO: later

        String toQuery()
        {
            String exprs = colMapping.entrySet().stream()
                    .map(e -> {
                        if (e.getKey().equalsIgnoreCase(e.getValue())) {
                            return e.getValue();
                        }
                        else {
                            return e.getValue() + " AS " + e.getKey();
                        }
                    })
                    .collect(Collectors.joining(", "));
            String query = "SELECT " + exprs + " FROM " + from;
            if (filter != null) {
                query = query + " WHERE " + filter;
            }
            if (groupByColumns != null && !groupByColumns.isEmpty()) {
                String groupByExpr = groupByColumns.stream().collect(Collectors.joining(","));
                query = query + " GROUP BY " + groupByExpr;
            }

            return query;
        }
    }
}
