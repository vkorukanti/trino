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
import io.trino.spi.block.IntArrayBlock;

import static java.util.Objects.requireNonNull;

public class TrinoDeltaIntVector
        extends AbstractTrinoDeltaVector
{
    private final IntArrayBlock trinoIntVector;

    public TrinoDeltaIntVector(IntArrayBlock trinoIntVector)
    {
        this.trinoIntVector = requireNonNull(trinoIntVector, "trinoIntVector is null");
    }

    @Override
    public Block getTrinoBlock()
    {
        return trinoIntVector;
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
        return trinoIntVector.isNull(rowId);
    }

    @Override
    public int getInt(int rowId)
    {
        return trinoIntVector.getInt(rowId, 0);
    }
}
