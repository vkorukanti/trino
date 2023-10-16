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

import io.delta.kernel.data.Row;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructType;
import io.trino.spi.block.Block;
import io.trino.spi.block.RowBlock;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class TrinoDeltaStructVector
        extends AbstractTrinoDeltaVector
{
    private final DataType deltaType;
    private final RowBlock rowBlock;

    public TrinoDeltaStructVector(DataType deltaType, RowBlock rowBlock)
    {
        this.deltaType = requireNonNull(deltaType, "deltaType is null");
        this.rowBlock = requireNonNull(rowBlock, "rowBlock is null");
    }

    @Override
    public Block getTrinoBlock()
    {
        return rowBlock;
    }

    @Override
    public DataType getDataType()
    {
        return deltaType;
    }

    @Override
    public boolean isNullAt(int rowId)
    {
        return rowBlock.isNull(rowId);
    }

    @Override
    public Row getStruct(int rowId)
    {
        return new StructRow(deltaType, rowBlock, rowId);
    }

    /**
     * Wrapper class to expose one member as a {@link Row}
     */
    private static class StructRow
            implements Row
    {
        private final DataType deltaType;
        private final RowBlock rowBlock;
        private final int rowId;

        StructRow(DataType deltaType, RowBlock rowBlock, int rowId)
        {
            this.deltaType = deltaType;
            this.rowBlock = requireNonNull(rowBlock, "rowBlock is null");
            this.rowId = rowId;
        }

        @Override
        public StructType getSchema()
        {
            return (StructType) deltaType;
        }

        @Override
        public boolean isNullAt(int ordinal)
        {
            return rowBlock.getChildren().get(ordinal).isNull(ordinal);
        }

        @Override
        public boolean getBoolean(int ordinal)
        {
            throw new UnsupportedOperationException("NYI");
        }

        @Override
        public byte getByte(int ordinal)
        {
            throw new UnsupportedOperationException("NYI");
        }

        @Override
        public short getShort(int ordinal)
        {
            throw new UnsupportedOperationException("NYI");
        }

        @Override
        public int getInt(int ordinal)
        {
            throw new UnsupportedOperationException("NYI");
        }

        @Override
        public long getLong(int ordinal)
        {
            throw new UnsupportedOperationException("NYI");
        }

        @Override
        public float getFloat(int ordinal)
        {
            throw new UnsupportedOperationException("NYI");
        }

        @Override
        public double getDouble(int ordinal)
        {
            throw new UnsupportedOperationException("NYI");
        }

        @Override
        public String getString(int ordinal)
        {
            throw new UnsupportedOperationException("NYI");
        }

        @Override
        public byte[] getBinary(int ordinal)
        {
            throw new UnsupportedOperationException("NYI");
        }

        @Override
        public Row getStruct(int ordinal)
        {
            throw new UnsupportedOperationException("NYI");
        }

        @Override
        public <T> List<T> getArray(int ordinal)
        {
            throw new UnsupportedOperationException("NYI");
        }

        @Override
        public <K, V> Map<K, V> getMap(int ordinal)
        {
            throw new UnsupportedOperationException("NYI");
        }
    }
}
