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
package io.trino.plugin.deltalake.kernel.data;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.data.vector.DefaultConstantVector;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;

import java.util.ArrayList;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.util.Objects.requireNonNull;

public class TrinoColumnarBatchWrapper
        implements ColumnarBatch
{
    private final StructType schema;
    private final Page page;

    public TrinoColumnarBatchWrapper(StructType schema, Page page)
    {
        this.schema = requireNonNull(schema, "schema is null");
        this.page = requireNonNull(page, "page is null");
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
    public ColumnarBatch withNewColumn(int i, StructField structField, ColumnVector columnVector)
    {
        switch (columnVector) {
            case DefaultConstantVector defaultConstantVector -> {
                DataType dataType = defaultConstantVector.getDataType();
                Page newPage = null;
                switch (dataType) {
                    case LongType longType -> {
                        Block block = RunLengthEncodedBlock.create(
                                BIGINT,
                                defaultConstantVector.getLong(0),
                                defaultConstantVector.getSize());
                        newPage = page.appendColumn(block);
                    }
                    case IntegerType integerType -> {
                        Block block = RunLengthEncodedBlock.create(
                                INTEGER,
                                defaultConstantVector.getInt(0),
                                defaultConstantVector.getSize());
                        newPage = page.appendColumn(block);
                    }
                    default -> throw new UnsupportedOperationException("Unsupported data type: " + dataType);
                }

                ArrayList<StructField> newStructFields = new ArrayList<>(schema.fields());
                newStructFields.ensureCapacity(schema.length() + 1);
                newStructFields.add(i, structField);
                StructType newSchema = new StructType(newStructFields);

                return new TrinoColumnarBatchWrapper(newSchema, newPage);
            }
            default -> throw new UnsupportedOperationException("Unsupported column vector: " + columnVector);
        }
    }

    @Override
    public ColumnarBatch withNewSchema(StructType newSchema)
    {
        return new TrinoColumnarBatchWrapper(newSchema, page);
    }

    @Override
    public ColumnarBatch withDeletedColumnAt(int ordinal)
    {
        return this;
    }

    @Override
    public ColumnVector getColumnVector(int ordinal)
    {
        Block block = page.getBlock(ordinal);
        return convertTrinoBlockToDeltaColumnVector(schema.at(ordinal).getDataType(), block);
    }

    private ColumnVector convertTrinoBlockToDeltaColumnVector(DataType deltaType, Block block)
    {
        return new TrinoColumnVectorWrapper(deltaType, block);
    }
}
