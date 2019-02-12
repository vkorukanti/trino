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
package com.facebook.presto.pinot;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.utils.DataTable;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Arrays;
import java.util.stream.IntStream;

import static com.facebook.presto.pinot.PinotErrorCode.PINOT_UNSUPPORTED_COLUMN_TYPE;
import static io.airlift.slice.Slices.utf8Slice;

public class PinotPageSourceUtils
{
    private PinotPageSourceUtils()
    {
    }

    /**
     * Generates the {@link com.facebook.presto.spi.block.Block} for the specific column from the given data table.
     *
     * <p>Based on the original Pinot column types, write as Presto-supported values to {@link com.facebook.presto.spi.block.BlockBuilder}, e.g.
     * FLOAT -> Double, INT -> Long, String -> Slice.
     *
     * @param blockBuilder blockBuilder for the current column
     * @param columnType type of the column
     * @param columnIdx column index
     */

    static void writeBlock(BlockBuilder blockBuilder, Type columnType, int columnIdx, FieldSpec.DataType pinotColumnType, DataTable dataTable)
    {
        Class<?> javaType = columnType.getJavaType();
        if (javaType.equals(boolean.class)) {
            writeBooleanBlock(blockBuilder, columnType, columnIdx, dataTable);
        }
        else if (javaType.equals(long.class)) {
            writeLongBlock(blockBuilder, columnType, columnIdx, pinotColumnType, dataTable);
        }
        else if (javaType.equals(double.class)) {
            writeDoubleBlock(blockBuilder, columnType, columnIdx, pinotColumnType, dataTable);
        }
        else if (javaType.equals(Slice.class)) {
            writeSliceBlock(blockBuilder, columnType, columnIdx, dataTable);
        }
        else {
            throw new PrestoException(
                    PINOT_UNSUPPORTED_COLUMN_TYPE,
                    String.format(
                            "Failed to write column. pinotColumnType %s, javaType %s",
                            pinotColumnType, javaType));
        }
    }

    static int writeBooleanBlock(BlockBuilder blockBuilder, Type columnType, int columnIdx, DataTable dataTable)
    {
        int completedBytes = 0;
        IntStream.rangeClosed(0, dataTable.getNumberOfRows() - 1).forEach(i ->
        {
            columnType.writeBoolean(blockBuilder, getBoolean(i, columnIdx, dataTable));
            //completedBytes++;
        });

        return completedBytes;
    }

    static void writeLongBlock(BlockBuilder blockBuilder, Type columnType, int columnIdx, FieldSpec.DataType pinotColumnType, DataTable dataTable)
    {
        IntStream.rangeClosed(0, dataTable.getNumberOfRows() - 1).forEach(i ->
        {
            columnType.writeLong(blockBuilder, getLong(i, columnIdx, dataTable));
            //completedBytes += pinotColumnType.size();
        });
    }

    static void writeDoubleBlock(BlockBuilder blockBuilder, Type columnType, int columnIdx, FieldSpec.DataType pinotColumnType, DataTable dataTable)
    {
        IntStream.rangeClosed(0, dataTable.getNumberOfRows() - 1).forEach(i ->
        {
            columnType.writeDouble(blockBuilder, getDouble(i, columnIdx, dataTable));
            //completedBytes += pinotColumnType.size();
        });
    }

    static void writeSliceBlock(BlockBuilder blockBuilder, Type columnType, int columnIdx, DataTable dataTable)
    {
        IntStream.rangeClosed(0, dataTable.getNumberOfRows() - 1).forEach(i ->
        {
            Slice slice = getSlice(i, columnIdx, dataTable);
            columnType.writeSlice(blockBuilder, slice, 0, slice.length());
            //completedBytes += slice.getBytes().length;
        });
    }

    static boolean getBoolean(int rowIdx, int colIdx, DataTable dataTable)
    {
        return dataTable.getBoolean(rowIdx, colIdx);
    }

    static long getLong(int rowIdx, int colIdx, DataTable dataTable)
    {
        FieldSpec.DataType dataType = dataTable.getDataSchema().getColumnType(colIdx);
        // Note columnType in the dataTable could be different from the original columnType in the columnHandle.
        // e.g. when original column type is int/long and aggregation value is requested, the returned dataType from Pinot would be double.
        // So need to cast it back to the original columnType.
        if (dataType.equals(FieldSpec.DataType.DOUBLE)) {
            return (long) dataTable.getDouble(rowIdx, colIdx);
        }
        if (dataType.equals(FieldSpec.DataType.INT)) {
            return (long) dataTable.getInt(rowIdx, colIdx);
        }
        else {
            return dataTable.getLong(rowIdx, colIdx);
        }
    }

    static double getDouble(int rowIdx, int colIdx, DataTable dataTable)
    {
        FieldSpec.DataType dataType = dataTable.getDataSchema().getColumnType(colIdx);
        if (dataType.equals(FieldSpec.DataType.FLOAT)) {
            return dataTable.getFloat(rowIdx, colIdx);
        }
        else {
            return dataTable.getDouble(rowIdx, colIdx);
        }
    }

    static Slice getSlice(int rowIdx, int colIdx, DataTable dataTable)
    {
        //checkColumnType(colIdx, VARCHAR);
        FieldSpec.DataType columnType = dataTable.getDataSchema().getColumnType(colIdx);
        switch (columnType) {
            case INT_ARRAY:
                int[] intArray = dataTable.getIntArray(rowIdx, colIdx);
                return utf8Slice(Arrays.toString(intArray));
            case LONG_ARRAY:
                long[] longArray = dataTable.getLongArray(rowIdx, colIdx);
                return utf8Slice(Arrays.toString(longArray));
            case FLOAT_ARRAY:
                float[] floatArray = dataTable.getFloatArray(rowIdx, colIdx);
                return utf8Slice(Arrays.toString(floatArray));
            case DOUBLE_ARRAY:
                double[] doubleArray = dataTable.getDoubleArray(rowIdx, colIdx);
                return utf8Slice(Arrays.toString(doubleArray));
            case STRING_ARRAY:
                String[] stringArray = dataTable.getStringArray(rowIdx, colIdx);
                return utf8Slice(Arrays.toString(stringArray));
            case STRING:
                String fieldStr = dataTable.getString(rowIdx, colIdx);
                if (fieldStr == null || fieldStr.isEmpty()) {
                    return Slices.EMPTY_SLICE;
                }
                return Slices.utf8Slice(fieldStr);
        }
        return Slices.EMPTY_SLICE;
    }
}
