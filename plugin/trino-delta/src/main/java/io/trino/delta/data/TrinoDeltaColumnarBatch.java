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
package io.trino.delta.data;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LazyBlock;
import io.trino.spi.block.LongArrayBlock;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class TrinoDeltaColumnarBatch
        implements ColumnarBatch
{
    private final StructType schema;
    private final Page page;
    private final Map<String, Integer> columnNameToIndexMap;

    public TrinoDeltaColumnarBatch(
            StructType schema,
            Page page,
            Map<String, Integer> columnNameToIndexMap)
    {
        this.schema = requireNonNull(schema, "schema is null");
        this.page = requireNonNull(page, "page is null");
        this.columnNameToIndexMap =
                requireNonNull(columnNameToIndexMap, "columnNameToIndexMap is null");
    }

    @Override
    public int getSize()
    {
        return page.getPositionCount();
    }

    @Override
    public StructType getSchema()
    {
        return schema;
    }

    @Override
    public ColumnarBatch slice(int start, int end)
    {
        return ColumnarBatch.super.slice(start, end);
    }

    @Override
    public CloseableIterator<Row> getRows()
    {
        return ColumnarBatch.super.getRows();
    }

    @Override
    public ColumnVector getColumnVector(int ordinal)
    {
        Block block = page.getBlock(ordinal);
        return convertTrinoBlockToDeltaColumnVector(block);
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

        throw new UnsupportedOperationException(
                "Not yet implemented: " + block.getClass().descriptorString());
    }
}
