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

import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.TestingColumnHandle;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.pipeline.ProjectPipelineNode;
import com.facebook.presto.spi.pipeline.PushdownExpression;
import com.facebook.presto.spi.pipeline.PushdownFunction;
import com.facebook.presto.spi.pipeline.PushdownInputColumn;
import com.facebook.presto.spi.pipeline.TableScanPipeline;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.testing.TestingHandleResolver;
import com.facebook.presto.testing.TestingMetadata;
import com.facebook.presto.testing.TestingMetadata.TestingTableHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.strictTableScan;

/**
 * WIP
 */
public class TestPushProjectIntoTableScan
        extends BaseRuleTest
{

    public TestPushProjectIntoTableScan()
    {
        super(new TestPlugin());
    }

    @Test
    public void testPushdownProjectIntoTableScan()
    {
        tester().assertThat(new PushProjectIntoTableScan(tester().getMetadata()))
                .on(p -> {
                    Symbol c1 = p.symbol("c1");
                    Symbol c2 = p.symbol("c2");
                    return p.project(
                            Assignments.of(
                                    p.symbol("c1"), c1.toSymbolReference(),
                                    p.symbol("e1"), p.expression("fun(c2)")),
                            p.tableScan(
                                    new TableHandle(
                                            new ConnectorId("test"),
                                            new TestingTableHandle()),
                                    ImmutableList.of(c1, c2),
                                    ImmutableMap.of(
                                            c1, new TestingColumnHandle("c1"),
                                            c2, new TestingColumnHandle("c2"))));
                })
                .matches(
                        strictTableScan("test",
                                ImmutableMap.of(
                                        "c1", "c1",
                                        "e1", "e1"))
                );
    }

    @Test
    public void testDoesntPushdownProjectIntoTableScan()
    {
        tester().assertThat(new PushProjectIntoTableScan(tester().getMetadata()))
                .on(p -> {
                    Symbol c1 = p.symbol("c1");
                    Symbol c2 = p.symbol("c2");
                    return p.project(
                            Assignments.of(
                                    p.symbol("c1"), c1.toSymbolReference(),
                                    p.symbol("e1"), p.expression("nonFun(c2)")),
                            p.tableScan(
                                    new TableHandle(
                                            new ConnectorId("test"),
                                            new TestingTableHandle()),
                                    ImmutableList.of(c1, c2),
                                    ImmutableMap.of(
                                            c1, new TestingColumnHandle("c1"),
                                            c2, new TestingColumnHandle("c2"))));
                }).doesNotFire();
    }

    private static class TestPlugin
            implements Plugin
    {
        @Override
        public Iterable<ConnectorFactory> getConnectorFactories()
        {
            return ImmutableList.of(
                    new ConnectorFactory()
                    {
                        @Override
                        public String getName()
                        {
                            return "test";
                        }

                        @Override
                        public ConnectorHandleResolver getHandleResolver()
                        {
                            return new TestingHandleResolver();
                        }

                        @Override
                        public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
                        {
                            return new TestConnector();
                        }
                    }
            );
        }
    }

    private static class TestConnector
            implements Connector
    {
        @Override
        public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
        {
            return new TestingMetadata()
            {
                @Override
                public Optional<TableScanPipeline> pushProjectIntoScan(ConnectorSession session, ConnectorTableHandle connectorTableHandle,
                        Optional<TableScanPipeline> currentPipeline, ProjectPipelineNode project)
                {
                    for (PushdownExpression projectExpr : project.getNodes()) {
                        if (projectExpr instanceof PushdownInputColumn) {
                            continue;
                        }
                        else if (projectExpr instanceof PushdownFunction) {
                            PushdownFunction function = (PushdownFunction) projectExpr;
                            if (!function.getName().equalsIgnoreCase("fun")) {
                                return Optional.empty();
                            }
                        }
                        else {
                            return Optional.empty();
                        }
                    }

                    List<ColumnHandle> newColumnHandles = new ArrayList<>();
                    for (String outputColumn : project.getOutputColumns()) {
                        newColumnHandles.add(new TestingColumnHandle(outputColumn));
                    }

                    TableScanPipeline pipeline = new TableScanPipeline();
                    pipeline.addPipeline(project, newColumnHandles);

                    return Optional.of(pipeline);
                }
            };
        }

        @Override
        public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ConnectorSplitManager getSplitManager()
        {
            throw new UnsupportedOperationException();
        }
    }
}
