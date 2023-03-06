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
package io.trino.delta;

import io.delta.standalone.core.DeltaScanTaskCore;
import io.delta.standalone.data.RowBatch;
import io.delta.standalone.data.RowRecord;
import io.delta.standalone.types.BinaryType;
import io.delta.standalone.types.BooleanType;
import io.delta.standalone.types.ByteType;
import io.delta.standalone.types.DataType;
import io.delta.standalone.types.DateType;
import io.delta.standalone.types.DecimalType;
import io.delta.standalone.types.DoubleType;
import io.delta.standalone.types.FloatType;
import io.delta.standalone.types.IntegerType;
import io.delta.standalone.types.LongType;
import io.delta.standalone.types.ShortType;
import io.delta.standalone.types.StringType;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;
import io.delta.standalone.types.TimestampType;
import io.delta.standalone.utils.CloseableIterator;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.hadoop.conf.Configuration;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class DeltaCoreTaskPageSource
        implements ConnectorPageSource
{
    private final HdfsEnvironment hdfsEnvironment;
    private final ConnectorIdentity identity;
    private final Configuration configuration;
    private final DeltaScanTaskCore task;
    private final List<DeltaColumnHandle> columns;

    private CloseableIterator<RowBatch> rowBatchIter;
    private boolean isFinished = false;

    public DeltaCoreTaskPageSource(
            HdfsEnvironment hdfsEnvironment,
            ConnectorIdentity identity,
            Configuration configuration,
            DeltaScanTaskCore task,
            List<DeltaColumnHandle> columns)
    {
        this.hdfsEnvironment = hdfsEnvironment;
        this.identity = identity;
        this.configuration = configuration;
        this.task = task;
        this.columns = columns;
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return isFinished;
    }

    @Override
    public Page getNextPage()
    {
        if (rowBatchIter == null) {
            rowBatchIter = task.getDataAsRows();
        }
        if (!rowBatchIter.hasNext()) {
            isFinished = true;
            return null;
        }
        RowBatch nextBatch = rowBatchIter.next();
        return convertDeltaRowsToPage(nextBatch);
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
    {
        closeQuietly(rowBatchIter);
    }

    private static void closeQuietly(Closeable client)
    {
        try {
            if (client != null) {
                client.close();
            }
        }
        catch (Exception e) {
            // ignored
        }
    }

    private static Page convertDeltaRowsToPage(RowBatch rowBatch)
    {
        // Dump method, but let's get it work for the hackathon
        List<RowRecord> rows = new ArrayList<>();
        CloseableIterator<RowRecord> rowIter = rowBatch.toRowIterator();
        rowIter.forEachRemaining(rows::add);
        closeQuietly(rowIter);

        if (rows.size() == 0) {
            return null;
        }

        StructType schema = rows.get(0).getSchema();
        List<Block> blocks = new ArrayList<>();
        for (StructField col : schema.getFields()) {
            Block block = convertCol(col.getDataType(), col.getName(), rows);
            blocks.add(block);
        }
        return new Page(blocks.toArray(new Block[0]));
    }

    private static Block convertCol(DataType deltaType, String colName, List<RowRecord> rows)
    {
        if (deltaType instanceof BinaryType) {
            throw new UnsupportedOperationException("TODO");
        }
        else if (deltaType instanceof BooleanType) {
            throw new UnsupportedOperationException("TODO");
        }
        else if (deltaType instanceof ByteType) {
            throw new UnsupportedOperationException("TODO");
        }
        else if (deltaType instanceof DateType) {
            throw new UnsupportedOperationException("TODO");
        }
        else if (deltaType instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) deltaType;
            throw new UnsupportedOperationException("TODO");
        }
        else if (deltaType instanceof DoubleType) {
            throw new UnsupportedOperationException("TODO");
        }
        else if (deltaType instanceof FloatType) {
            throw new UnsupportedOperationException("TODO");
        }
        else if (deltaType instanceof IntegerType) {
            int pageSize = rows.size();
            boolean[] valueIsNull = new boolean[pageSize];
            int[] values = new int[pageSize];

            for (int i = 0; i < pageSize; i++) {
                RowRecord row = rows.get(i);
                if (row.isNullAt(colName)) {
                    valueIsNull[i] = true;
                }
                else {
                    values[i] = row.getInt(colName);
                }
            }

            return new IntArrayBlock(pageSize, Optional.of(valueIsNull), values);
        }
        else if (deltaType instanceof LongType) {
            int pageSize = rows.size();
            boolean[] valueIsNull = new boolean[pageSize];
            long[] values = new long[pageSize];

            for (int i = 0; i < pageSize; i++) {
                RowRecord row = rows.get(i);
                if (row.isNullAt(colName)) {
                    valueIsNull[i] = true;
                }
                else {
                    values[i] = row.getLong(colName);
                }
            }

            return new LongArrayBlock(pageSize, Optional.of(valueIsNull), values);
        }
        else if (deltaType instanceof ShortType) {
            throw new UnsupportedOperationException("TODO");
        }
        else if (deltaType instanceof StringType) {
            throw new UnsupportedOperationException("TODO");
        }
        else if (deltaType instanceof TimestampType) {
            throw new UnsupportedOperationException("TODO");
        }
        throw new UnsupportedOperationException("TODO");
    }
}
