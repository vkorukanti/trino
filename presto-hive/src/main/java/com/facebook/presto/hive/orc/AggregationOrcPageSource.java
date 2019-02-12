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
package com.facebook.presto.hive.orc;

import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.memory.context.AggregatedMemoryContext;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.metadata.Footer;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.pipeline.AggregationPipelineNode;
import com.facebook.presto.spi.pipeline.AggregationPipelineNode.Aggregation;
import com.facebook.presto.spi.pipeline.PipelineNode;
import com.facebook.presto.spi.pipeline.TableScanPipeline;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.pipeline.AggregationPipelineNode.NodeType.AGGREGATE;
import static com.facebook.presto.spi.pipeline.PipelineNode.PipelineType.AGGREGATION;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class AggregationOrcPageSource
        implements ConnectorPageSource
{
    private final TableScanPipeline scanPipeline;
    private final Footer footer;
    private final OrcDataSource orcDataSource;
    private final TypeManager typeManager;
    private final AggregatedMemoryContext systemMemoryContext;

    private boolean completed;
    private boolean closed;

    public AggregationOrcPageSource(TableScanPipeline scanPipeline, Footer footer, OrcDataSource orcDataSource, TypeManager typeManager,
            AggregatedMemoryContext systemMemoryContext)
    {
        this.scanPipeline = requireNonNull(scanPipeline, "scanPipeline is null");
        this.footer = requireNonNull(footer, "footer is null");
        this.orcDataSource = requireNonNull(orcDataSource, "orcDataSource is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null");
    }

    @Override
    public long getCompletedBytes()
    {
        return orcDataSource.getReadBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return orcDataSource.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return completed;
    }

    @Override
    public Page getNextPage()
    {
        if (completed) {
            return null;
        }

        // Prepare the one required record by looking at the aggregations in pipeline and stats in footer
        final int batchSize = 1;

        List<PipelineNode> pipelineNodes = scanPipeline.getPipelineNodes();
        if (pipelineNodes == null || pipelineNodes.isEmpty() || pipelineNodes.get(0).getType() != AGGREGATION) {
            throw new IllegalArgumentException("scan pipeline is not valid");
        }

        AggregationPipelineNode aggregation = (AggregationPipelineNode) pipelineNodes.get(0);
        List<AggregationPipelineNode.Node> aggFunctions = aggregation.getNodes();
        List<ColumnHandle> outputColumnHandles = scanPipeline.getOutputColumnHandles();

        Map<String, Integer> columnNamesIndex = createColumnIndexMap();

        Block[] blocks = new Block[outputColumnHandles.size()];
        for (int fieldId = 0; fieldId < blocks.length; fieldId++) {
            if (aggFunctions.get(fieldId).getNodeType() != AGGREGATE) {
                throw new UnsupportedOperationException("unsupported aggregation expression: " + aggFunctions.get(fieldId).getNodeType());
            }

            Aggregation aggFunction = (Aggregation) aggFunctions.get(fieldId);
            HiveColumnHandle columnHandle = (HiveColumnHandle) outputColumnHandles.get(fieldId);
            Type type = typeManager.getType(columnHandle.getTypeSignature());

            BlockBuilder blockBuilder = type.createBlockBuilder(null, batchSize, 0);

            String inputCol = aggFunction.getInputs().isEmpty() ? null : aggFunction.getInputs().get(0);
            switch (aggFunction.getFunction().toLowerCase(ENGLISH)) {
                case "count":
                    if (inputCol == null) {
                        blockBuilder = blockBuilder.writeLong(footer.getNumberOfRows());
                    }
                    else {
                        writeNonNullCount(inputCol, columnNamesIndex, blockBuilder);
                    }
                    break;
                default:
                    throw new UnsupportedOperationException(aggFunction.getFunction() + " is not supported");
            }

            blocks[fieldId] = blockBuilder.build();
        }

        completed = true;

        return new Page(batchSize, blocks);
    }

    private Map<String, Integer> createColumnIndexMap()
    {
        Map<String, Integer> index = new HashMap<>();
        List<String> columns = footer.getTypes().get(0).getFieldNames();
        for (int i = 0; i < columns.size(); i++) {
            index.put(columns.get(i), i);
        }

        return index;
    }

    private void writeNonNullCount(String columnName, Map<String, Integer> columnIndex, BlockBuilder blockBuilder)
    {
        int index = columnIndex.get(columnName);
        ColumnStatistics columnStatistics = footer.getFileStats().get(index);
        blockBuilder.writeLong(columnStatistics.getNumberOfValues());
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return systemMemoryContext.getBytes();
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;

        try {
            orcDataSource.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
