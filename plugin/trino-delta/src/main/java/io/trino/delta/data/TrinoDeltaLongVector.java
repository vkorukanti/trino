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
import io.delta.kernel.types.LongType;
import io.trino.spi.block.Block;
import io.trino.spi.block.LongArrayBlock;

import static java.util.Objects.requireNonNull;

public class TrinoDeltaLongVector
        extends AbstractTrinoDeltaVector
{
    private final LongArrayBlock trinoLongVector;

    public TrinoDeltaLongVector(LongArrayBlock trinoLongVector)
    {
        this.trinoLongVector = requireNonNull(trinoLongVector, "trinoLongVector is null");
    }

    @Override
    public Block getTrinoBlock()
    {
        return trinoLongVector;
    }

    @Override
    public DataType getDataType()
    {
        return LongType.INSTANCE;
    }

    @Override
    public boolean isNullAt(int rowId)
    {
        return trinoLongVector.isNull(rowId);
    }

    @Override
    public long getLong(int rowId)
    {
        return trinoLongVector.getLong(rowId, 0);
    }
}
