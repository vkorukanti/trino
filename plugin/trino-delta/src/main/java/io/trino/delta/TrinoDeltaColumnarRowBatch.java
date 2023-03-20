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

import io.delta.standalone.data.ColumnVector;
import io.delta.standalone.data.ColumnarRowBatch;
import io.delta.standalone.types.DataType;
import io.delta.standalone.types.StructType;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LazyBlock;
import io.trino.spi.block.LongArrayBlock;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class TrinoDeltaColumnarRowBatch
        implements ColumnarRowBatch
{
    private final StructType schema;
    private final Page page;
    private final Map<String, Integer> columnNameToIndexMap;

    public TrinoDeltaColumnarRowBatch(
            StructType schema,
            Page page,
            Map<String, Integer> columnNameToIndexMap)
    {
        this.schema = requireNonNull(schema, "schema is null");
        this.page = requireNonNull(page, "page is null");
        this.columnNameToIndexMap = requireNonNull(columnNameToIndexMap, "columnNameToIndexMap is null");
    }

    @Override
    public int getNumRows()
    {
        return page.getPositionCount();
    }

    @Override
    public StructType schema()
    {
        return schema;
    }

    @Override
    public ColumnVector getColumnVector(String columnName)
    {
        Integer colIndex = columnNameToIndexMap.get(columnName);
        requireNonNull(colIndex, "Invalid column name: " + columnName);
        Block block = page.getBlock(colIndex);
        return convertTrinoBlockToDeltaColumnVector(block);
    }

    @Override
    public ColumnarRowBatch addColumnWithSingleValue(String columnName, DataType datatype, Object value)
    {
        // TODO: implement
        throw new UnsupportedOperationException("NYI");
    }

    @Override
    public void close()
    {
        // there is nothing to close in Trino vectors as they are heap based and managed by the JVM
    }

    private ColumnVector convertTrinoBlockToDeltaColumnVector(Block block)
    {
        if (block instanceof IntArrayBlock intArrayBlock) {
            return new TrinoDeltaIntVector(intArrayBlock);
        }

        if (block instanceof LongArrayBlock longArrayBlock) {
            return new TrinoDeltaLongVector(longArrayBlock);
        }

        if (block instanceof LazyBlock lazyBlock) {
            return new TrinoDeltaLazyVector(lazyBlock);
        }

        throw new UnsupportedOperationException("Not yet implemented: " + block.getClass().descriptorString());
    }
}
