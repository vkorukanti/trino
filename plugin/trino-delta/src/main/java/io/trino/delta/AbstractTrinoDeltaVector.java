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
import io.delta.standalone.data.ColumnarStruct;
import io.trino.spi.block.Block;

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
    public ColumnarStruct getStruct(int rowId)
    {
        throw new UnsupportedOperationException("TODO (improve): Invalid getValue type");
    }
}
