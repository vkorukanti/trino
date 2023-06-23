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

import io.trino.spi.block.Block;
import java.util.List;
import java.util.Map;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.DataType;

public abstract class AbstractTrinoDeltaVector
        implements ColumnVector
{
    @Override
    public boolean getBoolean(int rowId)
    {
        return false;
    }

    public abstract Block getTrinoBlock();

    @Override
    public byte getByte(int rowId)
    {
        throw new UnsupportedOperationException("TODO (improve): Invalid getValue type");
    }

    @Override
    public short getShort(int rowId)
    {
        throw new UnsupportedOperationException("TODO (improve): Invalid getValue type");
    }

    @Override
    public int getInt(int rowId)
    {
        throw new UnsupportedOperationException("TODO (improve): Invalid getValue type");
    }

    @Override
    public long getLong(int rowId)
    {
        throw new UnsupportedOperationException("TODO (improve): Invalid getValue type");
    }

    @Override
    public float getFloat(int rowId)
    {
        throw new UnsupportedOperationException("TODO (improve): Invalid getValue type");
    }

    @Override
    public double getDouble(int rowId)
    {
        throw new UnsupportedOperationException("TODO (improve): Invalid getValue type");
    }

    @Override
    public byte[] getBinary(int rowId)
    {
        throw new UnsupportedOperationException("TODO (improve): Invalid getValue type");
    }

    @Override
    public String getString(int rowId)
    {
        throw new UnsupportedOperationException("TODO (improve): Invalid getValue type");
    }

    @Override
    public Row getStruct(int rowId)
    {
        throw new UnsupportedOperationException("TODO (improve): Invalid getValue type");
    }

    @Override
    public <K, V> Map<K, V> getMap(int rowId)
    {
        throw new UnsupportedOperationException("TODO (improve): Invalid getValue type");
    }

    @Override
    public <T> List<T> getArray(int rowId)
    {
        throw new UnsupportedOperationException("TODO (improve): Invalid getValue type");
    }

    @Override
    public DataType getDataType()
    {
        throw new UnsupportedOperationException("TODO (improve): Invalid getValue type");
    }

    @Override
    public int getSize()
    {
        throw new UnsupportedOperationException("TODO (improve): Invalid getValue type");
    }

    @Override
    public void close()
    {
        // there is nothing to close in Trino vectors as they are heap based and managed by the JVM
    }

    @Override
    public boolean isNullAt(int rowId)
    {
        throw new UnsupportedOperationException("TODO (improve): Invalid getValue type");
    }
}
