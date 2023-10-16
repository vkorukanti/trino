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

import io.delta.kernel.types.DataType;
import io.trino.spi.block.ArrayBlock;
import io.trino.spi.block.Block;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class TrinoDeltaArrayVector
        extends AbstractTrinoDeltaVector
{
    private final DataType deltaType;
    private final ArrayBlock arrayBlock;

    public TrinoDeltaArrayVector(DataType deltaType, ArrayBlock arrayBlock)
    {
        this.deltaType = requireNonNull(deltaType, "deltaType is null");
        this.arrayBlock = requireNonNull(arrayBlock, "arrayBlock is null");
    }

    @Override
    public Block getTrinoBlock()
    {
        return arrayBlock;
    }

    @Override
    public boolean isNullAt(int rowId)
    {
        return arrayBlock.isNull(rowId);
    }

    @Override
    public <T> List<T> getArray(int rowId)
    {
        throw new UnsupportedOperationException("NYI");
    }
}
