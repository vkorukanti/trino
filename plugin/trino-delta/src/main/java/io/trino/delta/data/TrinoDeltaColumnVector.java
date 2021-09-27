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

import io.airlift.slice.Slice;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.DataType;
import io.trino.spi.block.Block;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class TrinoDeltaColumnVector
        extends AbstractTrinoDeltaVector
{
    private final DataType deltaType;
    private final Block trinoBlock;

    public TrinoDeltaColumnVector(DataType deltaType, Block trinoBlock)
    {
        this.trinoBlock = requireNonNull(trinoBlock, "trinoBlock is null");
        this.deltaType = requireNonNull(deltaType, "deltaType is null");
    }

    @Override
    public Block getTrinoBlock()
    {
        return trinoBlock;
    }

    @Override
    public int getSize()
    {
        return trinoBlock.getPositionCount();
    }

    @Override
    public DataType getDataType()
    {
        return deltaType;
    }

    @Override
    public boolean isNullAt(int rowId)
    {
        return trinoBlock.isNull(rowId);
    }

    @Override
    public boolean getBoolean(int rowId)
    {
        return trinoBlock.getByte(rowId, 0) != 0;
    }

    @Override
    public byte getByte(int rowId)
    {
        return trinoBlock.getByte(rowId, 0);
    }

    @Override
    public short getShort(int rowId)
    {
        return trinoBlock.getShort(rowId, 0);
    }

    @Override
    public long getLong(int rowId)
    {
        return trinoBlock.getLong(rowId, 0);
    }

    @Override
    public int getInt(int rowId)
    {
        return trinoBlock.getInt(rowId, 0);
    }

    @Override
    public float getFloat(int rowId)
    {
        return trinoBlock.getInt(rowId, 0);
    }

    @Override
    public double getDouble(int rowId)
    {
        return trinoBlock.getLong(rowId, 0);
    }

    @Override
    public byte[] getBinary(int rowId)
    {
        int positionLength = trinoBlock.getSliceLength(rowId);
        Slice slice = trinoBlock.getSlice(rowId, 0, positionLength);
        return slice.byteArray();
    }

    @Override
    public String getString(int rowId)
    {
        int positionLength = trinoBlock.getSliceLength(rowId);
        Slice slice = trinoBlock.getSlice(rowId, 0, positionLength);
        return slice.toStringUtf8();
    }

    @Override
    public <K, V> Map<K, V> getMap(int rowId)
    {
        throw new UnsupportedOperationException("NYI");
    }

    @Override
    public Row getStruct(int rowId)
    {
        throw new UnsupportedOperationException("NYI");
    }

    @Override
    public <T> List<T> getArray(int rowId)
    {
        throw new UnsupportedOperationException("NYI");
    }
}
