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

import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.common.response.ServerInstance;
import com.linkedin.pinot.common.utils.DataTable;
import io.airlift.log.Logger;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static com.facebook.presto.pinot.PinotQueryGenerator.getPinotQuery;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * This class retrieves Pinot data from a Pinot client, and re-constructs the data into Presto Pages.
 */

public class PinotPageSource
        implements ConnectorPageSource
{
    private static final Logger log = Logger.get(PinotPageSource.class);
    // Stores the mapping between pinot column name and the column index
    Map<String, Integer> pinotColumnNameIndexMap = new HashMap<>();
    private List<PinotColumnHandle> columnHandles;
    private List<Type> columnTypes;
    private PinotConfig pinotConfig;
    private PinotSplit split;
    private PinotScatterGatherQueryClient pinotQueryClient;
    // dataTableList stores the dataTable returned from each server. Each dataTable is constructed to a Page, and then destroyed to save memory.
    private LinkedList<PinotDataTableWithSize> dataTableList = new LinkedList<>();
    private long completedBytes;
    private long readTimeNanos;
    private long estimatedMemoryUsageInBytes;
    private PinotDataTableWithSize currentDataTable;
    private boolean closed;
    private boolean isPinotDataFetched;

    public PinotPageSource(PinotConfig pinotConfig, PinotScatterGatherQueryClient pinotQueryClient, PinotSplit split, List<PinotColumnHandle> columnHandles)
    {
        requireNonNull(split, "split is null");
        this.pinotConfig = requireNonNull(pinotConfig, "pinotConfig is null");
        this.split = requireNonNull(split, "split is null");
        this.pinotQueryClient = requireNonNull(pinotQueryClient, "pinotQueryClient is null");
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return estimatedMemoryUsageInBytes;
    }

    /**
     * @return true if is closed or all Pinot data have been processed.
     */
    @Override
    public boolean isFinished()
    {
        return closed || (isPinotDataFetched && dataTableList.isEmpty());
    }

    /**
     * Iterate through each Pinot {@link com.linkedin.pinot.common.utils.DataTable}, and construct a {@link com.facebook.presto.spi.Page} out of it.
     *
     * @return constructed page for pinot data.
     */
    @Override
    public Page getNextPage()
    {
        if (isFinished()) {
            close();
            return null;
        }
        if (!isPinotDataFetched) {
            fetchPinotData();
        }
        // To reduce memory usage, remove dataTable from dataTableList once it's processed.
        if (currentDataTable != null) {
            estimatedMemoryUsageInBytes -= currentDataTable.getEstimatedSizeInBytes();
        }
        if (dataTableList.size() == 0) {
            close();
            return null;
        }
        currentDataTable = dataTableList.pop();

        PageBuilder pageBuilder = new PageBuilder(columnTypes);
        // Note that declared positions in the Page should be the same with number of rows in each Block
        pageBuilder.declarePositions(currentDataTable.getDataTable().getNumberOfRows());
        for (int columnHandleIdx = 0; columnHandleIdx < columnHandles.size(); columnHandleIdx++) {
            BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(columnHandleIdx);
            Type columnType = columnTypes.get(columnHandleIdx);
            // Write a block for each column in the original order.
            int columnIdx = pinotColumnNameIndexMap.get(columnHandles.get(columnHandleIdx).getColumnName());
            PinotPageSourceUtils.writeBlock(blockBuilder, columnType,
                    columnIdx,
                    currentDataTable.getDataTable().getDataSchema().getColumnType(columnIdx),
                    currentDataTable.getDataTable());
        }
        Page page = pageBuilder.build();
        return page;
    }

    /**
     * Fetch data from Pinot for the current split and store the {@link com.linkedin.pinot.common.utils.DataTable} returned from each Pinto server.
     */
    private void fetchPinotData()
    {
        log.debug("Fetching data from Pinot for table %s, segment %s", split.getTableName().get(), split.getSegment().get());
        long startTimeNanos = System.nanoTime();
        int idx = 0;
        for (PinotColumnHandle columnHandle : columnHandles) {
            pinotColumnNameIndexMap.put(columnHandle.getColumnName(), idx++);
        }
        String pinotQuery = getPinotQuery(pinotConfig, columnHandles, split.getPinotFilter().get(), split.getTimeFilter().get(), split.getTableName().get(), split.getLimit());
        Map<ServerInstance, DataTable> dataTableMap = pinotQueryClient.queryPinotServerForDataTable(pinotQuery, split.getHost().get(), split.getSegment().get());
        dataTableMap.values().stream()
                // ignore empty tables and tables with 0 rows
                .filter(table -> table != null && table.getNumberOfRows() > 0)
                .forEach(dataTable ->
                {
                    // Store each dataTable which will later be constructed into Pages.
                    // Also update estimatedMemoryUsage, mostly represented by the size of all dataTables, using numberOfRows and fieldTypes combined as an estimate
                    int estimatedTableSizeInBytes = IntStream.rangeClosed(0, dataTable.getDataSchema().size() - 1)
                            .map(i -> getEstimatedColumnSizeInBytes(dataTable.getDataSchema().getColumnType(i)) * dataTable.getNumberOfRows())
                            .reduce(0, Integer::sum);
                    dataTableList.add(new PinotDataTableWithSize(dataTable, estimatedTableSizeInBytes));
                    estimatedMemoryUsageInBytes += estimatedTableSizeInBytes;
                });
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        columnHandles
                .stream()
                .map(columnHandle -> columnHandle.getColumnType())
                .forEach(types::add);
        this.columnTypes = types.build();
        readTimeNanos = System.nanoTime() - startTimeNanos;
        isPinotDataFetched = true;
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;
    }

    Type getType(int colIdx)
    {
        checkArgument(colIdx < columnHandles.size(), "Invalid field index");
        return columnHandles.get(colIdx).getColumnType();
    }

    /**
     * Get estimated size in bytes for the Pinot column.
     * Deterministic for numeric fields; use estimate for other types to save calculation.
     *
     * @param dataType FieldSpec.dataType for Pinot column.
     * @return estimated size in bytes.
     */
    private int getEstimatedColumnSizeInBytes(DataType dataType)
    {
        if (dataType.isNumber()) {
            return dataType.getStoredType().size();
        }
        else {
            return pinotConfig.getEstimatedSizeInBytesForNonNumericColumn();
        }
    }

    void checkColumnType(int colIdx, Type expected)
    {
        Type actual = getType(colIdx);
        checkArgument(actual.equals(expected), "Expected column %s to be type %s but is %s", colIdx, expected, actual);
    }

    private class PinotDataTableWithSize
    {
        DataTable dataTable;
        int estimatedSizeInBytes;

        PinotDataTableWithSize(DataTable dataTable, int estimatedSizeInBytes)
        {
            this.dataTable = dataTable;
            this.estimatedSizeInBytes = estimatedSizeInBytes;
        }

        DataTable getDataTable()
        {
            return dataTable;
        }

        int getEstimatedSizeInBytes()
        {
            return estimatedSizeInBytes;
        }
    }
}
