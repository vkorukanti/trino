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

import io.delta.standalone.types.DataType;
import io.delta.standalone.types.IntegerType;
import io.trino.spi.block.Block;
import io.trino.spi.block.LazyBlock;

import static java.util.Objects.requireNonNull;

public class TrinoDeltaLazyVector
        extends AbstractTrinoDeltaVector
{
    private final LazyBlock trinoLazyBlock;

    public TrinoDeltaLazyVector(LazyBlock trinoLazyBlock)
    {
        this.trinoLazyBlock = requireNonNull(trinoLazyBlock, "trinoLazyBlock is null");
    }

    @Override
    public Block getTrinoBlock()
    {
        return trinoLazyBlock;
    }

    @Override
    public DataType getDataType()
    {
        return new IntegerType();
    }

    @Override
    public void close()
    {
        // there is nothing to close in Trino vectors as they are heap based and managed by the JVM
    }

    @Override
    public boolean isNullAt(int rowId)
    {
        return trinoLazyBlock.isNull(rowId);
    }

    @Override
    public long getLong(int rowId)
    {
        return trinoLazyBlock.getLong(rowId, 0);
    }

    @Override
    public int getInt(int rowId)
    {
        return trinoLazyBlock.getInt(rowId, 0);
    }
}
