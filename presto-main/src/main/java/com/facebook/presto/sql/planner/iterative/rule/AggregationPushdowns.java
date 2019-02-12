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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.pipeline.AggregationPipelineNode;
import com.facebook.presto.spi.pipeline.TableScanPipeline;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.SymbolReference;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.matching.Pattern.empty;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.FINAL;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.gatheringExchange;
import static com.facebook.presto.sql.planner.plan.Patterns.Aggregation.groupingColumns;
import static com.facebook.presto.sql.planner.plan.Patterns.Aggregation.step;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.facebook.presto.sql.planner.plan.Patterns.exchange;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.facebook.presto.sql.planner.plan.Patterns.tableScan;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class AggregationPushdowns
{
    /**
     * Convert planner based objects into connector format
     */
    private static Optional<AggregationPipelineNode> inConnectorFormat(List<Symbol> aggOutputSymbols, Map<Symbol, AggregationNode.Aggregation> aggregations,
            List<Symbol> groupByKeys, List<Symbol> finalOutputSymbols, TypeProvider typeProvider)
    {
        int groupByKeyIndex = 0;
        AggregationPipelineNode aggPipelineNode = new AggregationPipelineNode();
        for (int fieldId = 0; fieldId < finalOutputSymbols.size(); fieldId++) {
            Symbol aggOutputSymbol = aggOutputSymbols.get(fieldId);
            Symbol finalOutputSymbol = finalOutputSymbols.get(fieldId);
            AggregationNode.Aggregation agg = aggregations.get(aggOutputSymbol);

            if (agg != null) {
                FunctionCall aggFunction = agg.getCall();
                if (!areInputsSymbolReferences(aggFunction)) {
                    return Optional.empty();
                }
                // aggregation output
                aggPipelineNode.addAggregation(
                        getInputColumnNames(aggFunction),
                        agg.getCall().getName().toString(),
                        finalOutputSymbol.getName(),
                        typeProvider.get(finalOutputSymbol));
            }
            else {
                // group by output
                Symbol inputSymbol = groupByKeys.get(groupByKeyIndex);
                aggPipelineNode.addGroupBy(inputSymbol.getName(), finalOutputSymbol.getName(), typeProvider.get(aggOutputSymbol));
                groupByKeyIndex++;
            }
        }

        return Optional.of(aggPipelineNode);
    }

    private static boolean areInputsSymbolReferences(FunctionCall function)
    {
        // make sure all function inputs are null or symbol references
        return function.getArguments().stream()
                .allMatch(arg -> arg instanceof SymbolReference);
    }

    /**
     * Extract inputs column references from given function arguments
     */
    private static List<String> getInputColumnNames(FunctionCall function)
    {
        checkArgument(function.getArguments().stream()
                .allMatch(argument -> argument == null || argument instanceof SymbolReference));

        return function.getArguments().stream()
                .filter(argument -> argument != null)
                .map(argument -> ((SymbolReference) argument).getName())
                .collect(Collectors.toList());
    }

    private static Map<Symbol, ColumnHandle> createAssignments(List<ColumnHandle> outputColumnHandles, List<Symbol> outputSymbols)
    {
        Map<Symbol, ColumnHandle> assignments = new HashMap<>();
        for (int i = 0; i < outputSymbols.size(); i++) {
            assignments.put(outputSymbols.get(i), outputColumnHandles.get(i));
        }

        return assignments;
    }

    /**
     * Pushes partial aggregation into table scan. Useful in connectors which can compute faster than Presto or
     * already have pre-computed partial un-grouped aggregations by split level
     *
     * <p>
     * From:
     * <pre>
     * - Aggregation (FINAL)
     *   - Exchange (Local)
     *      - Exchange (Remote)
     *          - Aggregation (PARTIAL)
     *              - TableScan
     * </pre>
     * To:
     * <pre>
     * - TableScan (with aggregation pushed into the scan)
     * </pre>
     * <p>
     */
    public static class PushAggregationIntoTableScan
            implements Rule<AggregationNode>
    {
        private static final Capture<TableScanNode> TABLE_SCAN = newCapture();
        private static final Capture<AggregationNode> PARTIAL_AGGREGATION = newCapture();
        private static final Pattern<AggregationNode> PATTERN = aggregation()
                // Only consider FINAL un-grouped aggregations
                .with(step().equalTo(FINAL))
                // Only consider aggregations without ORDER BY clause
                .matching(node -> !node.hasOrderings())
                .with(
                        source().matching(exchange().with(
                                source().matching(exchange().with(
                                        source().matching(aggregation().capturedAs(PARTIAL_AGGREGATION).with(step().equalTo(PARTIAL)).with(
                                                source().matching(tableScan().capturedAs(TABLE_SCAN)))))))));

        private final Metadata metadata;

        public PushAggregationIntoTableScan(Metadata metadata)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
        }

        @Override
        public Pattern<AggregationNode> getPattern()
        {
            return PATTERN;
        }

        @Override
        public boolean isEnabled(Session session)
        {
            return SystemSessionProperties.isPushdownAggregationsIntoScan(session);
        }

        @Override
        public Result apply(AggregationNode aggregation, Captures captures, Context context)
        {
            TableScanNode scanNode = captures.get(TABLE_SCAN);
            AggregationNode partialAggregation = captures.get(PARTIAL_AGGREGATION);

            Optional<AggregationPipelineNode> aggPipelineNode = inConnectorFormat(
                    partialAggregation.getOutputSymbols(),
                    partialAggregation.getAggregations(),
                    partialAggregation.getGroupingKeys(),
                    aggregation.getOutputSymbols(),
                    context.getSymbolAllocator().getTypes());

            if (!aggPipelineNode.isPresent()) {
                return Result.empty();
            }

            Optional<TableScanPipeline> newScanPipeline = metadata.pushAggregationsIntoScan(
                    context.getSession(), scanNode.getTable(), scanNode.getScanPipeline(), aggPipelineNode.get());

            if (newScanPipeline.isPresent()) {
                PlanNodeIdAllocator idAllocator = context.getIdAllocator();

                TableScanNode newScanNode = new TableScanNode(
                        idAllocator.getNextId(),
                        scanNode.getTable(),
                        aggregation.getOutputSymbols(),
                        createAssignments(newScanPipeline.get().getOutputColumnHandles(), aggregation.getOutputSymbols()),
                        scanNode.getLayout(),
                        scanNode.getCurrentConstraint(),
                        scanNode.getEnforcedConstraint(),
                        newScanPipeline);

                // add a REMOTE GATHER exchange in between so, that the distribution of the fragment having scan and fragment having output node are different
                ExchangeNode exchangeNode = gatheringExchange(idAllocator.getNextId(), REMOTE, newScanNode);

                return Result.ofPlanNode(exchangeNode);
            }

            return Result.empty();
        }
    }

    /**
     * Pushes partial aggregation into table scan. Useful in connectors which can compute faster than Presto or
     * already have pre-computed partial un-grouped aggregations by split level
     *
     * <p>
     * From:
     * <pre>
     * - Aggregation (PARTIAL)
     *   - TableScan
     * </pre>
     * To:
     * <pre>
     * - TableScan (with aggregation pushed into the scan)
     * </pre>
     * <p>
     */
    public static class PushPartialAggregationIntoTableScan
            implements Rule<AggregationNode>
    {
        private static final Capture<TableScanNode> TABLE_SCAN = newCapture();
        private static final Pattern<AggregationNode> PATTERN = aggregation()
                // Only consider PARTIAL un-grouped aggregations
                .with(step().equalTo(PARTIAL))
                .with(empty(groupingColumns()))
                // Only consider aggregations without ORDER BY clause
                .matching(node -> !node.hasOrderings())
                .with(source().matching(tableScan().capturedAs(TABLE_SCAN)));

        private final Metadata metadata;

        public PushPartialAggregationIntoTableScan(Metadata metadata)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
        }

        @Override
        public Pattern<AggregationNode> getPattern()
        {
            return PATTERN;
        }

        @Override
        public boolean isEnabled(Session session)
        {
            return SystemSessionProperties.isPushdownPartialAggregationsIntoScan(session);
        }

        @Override
        public Result apply(AggregationNode aggregation, Captures captures, Context context)
        {
            TableScanNode scanNode = captures.get(TABLE_SCAN);

            Optional<AggregationPipelineNode> aggPipelineNode = inConnectorFormat(
                    aggregation.getOutputSymbols(),
                    aggregation.getAggregations(),
                    aggregation.getGroupingKeys(),
                    aggregation.getOutputSymbols(),
                    context.getSymbolAllocator().getTypes());

            if (!aggPipelineNode.isPresent()) {
                return Result.empty();
            }

            Optional<TableScanPipeline> newScanPipeline = metadata.pushPartialAggregationsIntoScan(
                    context.getSession(), scanNode.getTable(), scanNode.getScanPipeline(), aggPipelineNode.get());

            if (newScanPipeline.isPresent()) {
                return Result.ofPlanNode(new TableScanNode(
                        context.getIdAllocator().getNextId(),
                        scanNode.getTable(),
                        aggregation.getOutputSymbols(),
                        createAssignments(newScanPipeline.get().getOutputColumnHandles(), aggregation.getOutputSymbols()),
                        scanNode.getLayout(),
                        scanNode.getCurrentConstraint(),
                        scanNode.getEnforcedConstraint(),
                        newScanPipeline));
            }

            return Result.empty();
        }
    }
}
