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
import io.trino.spi.block.Block;
import io.trino.spi.block.MapBlock;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class TrinoDeltaMapVector
        extends AbstractTrinoDeltaVector
{
    private final DataType deltaType;
    private final MapBlock mapBlock;

    public TrinoDeltaMapVector(DataType deltaType, MapBlock mapBlock)
    {
        this.deltaType = requireNonNull(deltaType, "deltaType is null");
        this.mapBlock = requireNonNull(mapBlock, "mapBlock is null");
    }

    @Override
    public Block getTrinoBlock()
    {
        return mapBlock;
    }

    @Override
    public boolean isNullAt(int rowId)
    {
        return mapBlock.isNull(rowId);
    }

    @Override
    public <K, V> Map<K, V> getMap(int rowId)
    {
        throw new UnsupportedOperationException("NYI");
    }
}
