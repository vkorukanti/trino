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

import com.google.common.collect.ImmutableList;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampType;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.NamedTypeSignature;
import io.trino.spi.type.RowFieldName;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.TypeSignatureParameter;

import java.util.Locale;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.delta.DeltaErrorCode.DELTA_UNSUPPORTED_COLUMN_TYPE;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.StandardTypes.ARRAY;
import static io.trino.spi.type.StandardTypes.MAP;
import static io.trino.spi.type.StandardTypes.ROW;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.String.format;

/**
 * Contains utility methods to convert Delta data types (and data values) to Trino data types (and data values)
 */
public class DeltaTypeUtils
{
    private DeltaTypeUtils()
    {
    }

    /**
     * Convert given Delta data type to Trino data type signature.
     *
     * @param tableName Used in error messages when an unsupported data type is encountered.
     * @param columnName Used in error messages when an unsupported data type is encountered.
     * @param deltaType Data type to convert
     */
    public static TypeSignature convertDeltaType(
            SchemaTableName tableName,
            String columnName,
            DataType deltaType)
    {
        checkArgument(deltaType != null);

        if (deltaType instanceof StructType) {
            StructType deltaStructType = (StructType) deltaType;
            ImmutableList.Builder<TypeSignatureParameter> typeSignatureBuilder =
                    ImmutableList.builder();
            deltaStructType.fields().stream()
                    .forEach(field -> {
                        String rowFieldName = field.getName().toLowerCase(Locale.US);
                        TypeSignature rowFieldType = convertDeltaType(
                                tableName,
                                columnName + "." + field.getName(),
                                field.getDataType());
                        typeSignatureBuilder.add(
                                TypeSignatureParameter.namedTypeParameter(
                                        new NamedTypeSignature(
                                                Optional.of(new RowFieldName(rowFieldName)),
                                                rowFieldType)));
                    });
            return new TypeSignature(ROW, typeSignatureBuilder.build());
        }
        else if (deltaType instanceof ArrayType) {
            ArrayType deltaArrayType = (ArrayType) deltaType;
            TypeSignature elementType = convertDeltaType(
                    tableName,
                    columnName,
                    deltaArrayType.getElementType());
            return new TypeSignature(
                    ARRAY,
                    ImmutableList.of(TypeSignatureParameter.typeParameter(elementType)));
        }
        else if (deltaType instanceof MapType) {
            MapType deltaMapType = (MapType) deltaType;
            TypeSignature keyType =
                    convertDeltaType(tableName, columnName, deltaMapType.getKeyType());
            TypeSignature valueType =
                    convertDeltaType(tableName, columnName, deltaMapType.getValueType());
            return new TypeSignature(MAP, ImmutableList.of(
                    TypeSignatureParameter.typeParameter(keyType),
                    TypeSignatureParameter.typeParameter(valueType)));
        }

        return convertDeltaPrimitiveType(tableName, columnName, deltaType)
                .getTypeSignature();
    }

    private static Type convertDeltaPrimitiveType(SchemaTableName tableName,
            String columnName, DataType deltaType)
    {
        if (deltaType instanceof BinaryType) {
            return VARBINARY;
        }
        if (deltaType instanceof BooleanType) {
            return BOOLEAN;
        }
        else if (deltaType instanceof ByteType) {
            return TINYINT;
        }
        else if (deltaType instanceof DateType) {
            return DATE;
        }
        else if (deltaType instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) deltaType;
            return createDecimalType(decimalType.getPrecision(), decimalType.getScale());
        }
        else if (deltaType instanceof DoubleType) {
            return DOUBLE;
        }
        else if (deltaType instanceof FloatType) {
            return REAL;
        }
        else if (deltaType instanceof IntegerType) {
            return INTEGER;
        }
        else if (deltaType instanceof LongType) {
            return BIGINT;
        }
        else if (deltaType instanceof ShortType) {
            return SMALLINT;
        }
        else if (deltaType instanceof StringType) {
            return createUnboundedVarcharType();
        }
        else if (deltaType instanceof TimestampType) {
            return TIMESTAMP_MICROS;
        }

        throw new TrinoException(DELTA_UNSUPPORTED_COLUMN_TYPE,
                format("Column '%s' in Delta table %s contains unsupported data type: %s",
                        columnName,
                        tableName,
                        deltaType));
    }
}
