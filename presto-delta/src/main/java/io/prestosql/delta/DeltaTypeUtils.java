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
package io.prestosql.delta;

import com.google.common.collect.ImmutableList;
import io.delta.standalone.types.ArrayType;
import io.delta.standalone.types.BinaryType;
import io.delta.standalone.types.BooleanType;
import io.delta.standalone.types.ByteType;
import io.delta.standalone.types.DataType;
import io.delta.standalone.types.DateType;
import io.delta.standalone.types.DecimalType;
import io.delta.standalone.types.DoubleType;
import io.delta.standalone.types.FloatType;
import io.delta.standalone.types.IntegerType;
import io.delta.standalone.types.LongType;
import io.delta.standalone.types.MapType;
import io.delta.standalone.types.ShortType;
import io.delta.standalone.types.StringType;
import io.delta.standalone.types.StructType;
import io.delta.standalone.types.TimestampType;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.Decimals;
import io.prestosql.spi.type.NamedTypeSignature;
import io.prestosql.spi.type.RowFieldName;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.delta.DeltaErrorCode.DELTA_INVALID_PARTITION_VALUE;
import static io.prestosql.delta.DeltaErrorCode.DELTA_UNSUPPORTED_COLUMN_TYPE;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.StandardTypes.ARRAY;
import static io.prestosql.spi.type.StandardTypes.MAP;
import static io.prestosql.spi.type.StandardTypes.ROW;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.Double.parseDouble;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.parseFloat;
import static java.lang.Long.parseLong;
import static java.lang.String.format;

/**
 * Contains utility methods to convert Delta data types (and data values) to Presto data types (and data values)
 */
public class DeltaTypeUtils
{
    private DeltaTypeUtils()
    {
    }

    /**
     * Convert given Delta data type to Presto data type signature.
     *
     * @param tableName Used in error messages when an unsupported data type is encountered.
     * @param columnName Used in error messages when an unsupported data type is encountered.
     * @param deltaType Data type to convert
     * @return
     */
    public static TypeSignature convertDeltaDataTypePrestoDataType(SchemaTableName tableName, String columnName, DataType deltaType)
    {
        checkArgument(deltaType != null);

        if (deltaType instanceof StructType) {
            StructType deltaStructType = (StructType) deltaType;
            ImmutableList.Builder<TypeSignatureParameter> typeSignatureBuilder = ImmutableList.builder();
            Arrays.stream(deltaStructType.getFields())
                    .forEach(field -> {
                        String rowFieldName = field.getName().toLowerCase(Locale.US);
                        TypeSignature rowFieldType = convertDeltaDataTypePrestoDataType(
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
            TypeSignature elementType = convertDeltaDataTypePrestoDataType(
                    tableName,
                    columnName,
                    deltaArrayType.getElementType());
            return new TypeSignature(
                    ARRAY,
                    ImmutableList.of(TypeSignatureParameter.typeParameter(elementType)));
        }
        else if (deltaType instanceof MapType) {
            MapType deltaMapType = (MapType) deltaType;
            TypeSignature keyType = convertDeltaDataTypePrestoDataType(tableName, columnName, deltaMapType.getKeyType());
            TypeSignature valueType = convertDeltaDataTypePrestoDataType(tableName, columnName, deltaMapType.getValueType());
            return new TypeSignature(MAP, ImmutableList.of(
                    TypeSignatureParameter.typeParameter(keyType),
                    TypeSignatureParameter.typeParameter(valueType)));
        }

        return convertDeltaPrimitiveTypeToPrestoPrimitiveType(tableName, columnName, deltaType).getTypeSignature();
    }

    public static Object convertPartitionValue(
            String partitionColumnName,
            String partitionValue,
            TypeSignature partitionDataType)
    {
        if (partitionValue == null) {
            return null;
        }

        String typeBase = partitionDataType.getBase();
        try {
            switch (typeBase) {
                case StandardTypes.TINYINT:
                case StandardTypes.SMALLINT:
                case StandardTypes.INTEGER:
                case StandardTypes.BIGINT:
                    return parseLong(partitionValue);
                case StandardTypes.REAL:
                    return (long) floatToRawIntBits(parseFloat(partitionValue));
                case StandardTypes.DOUBLE:
                    return parseDouble(partitionValue);
                case StandardTypes.VARCHAR:
                case StandardTypes.VARBINARY:
                    return utf8Slice(partitionValue);
                case StandardTypes.DATE:
                    return LocalDate.parse(partitionValue, DateTimeFormatter.ISO_LOCAL_DATE).toEpochDay();
                case StandardTypes.TIMESTAMP:
                    return Timestamp.valueOf(partitionValue).toLocalDateTime().toEpochSecond(ZoneOffset.UTC) * 1000;
                case StandardTypes.BOOLEAN:
                    checkArgument(partitionValue.equalsIgnoreCase("true") || partitionValue.equalsIgnoreCase("false"));
                    return Boolean.valueOf(partitionValue);
                case StandardTypes.DECIMAL:
                    return Decimals.parse(partitionValue).getObject();
                default:
                    throw new PrestoException(DELTA_UNSUPPORTED_COLUMN_TYPE,
                            format("Unsupported data type '%s' for partition column %s", partitionDataType, partitionColumnName));
            }
        }
        catch (IllegalArgumentException | DateTimeParseException e) {
            throw new PrestoException(DELTA_INVALID_PARTITION_VALUE,
                    format("Can not parse partition value '%s' of type '%s' for partition column '%s'",
                            partitionValue, partitionDataType, partitionColumnName));
        }
    }

    private static Type convertDeltaPrimitiveTypeToPrestoPrimitiveType(SchemaTableName tableName, String columnName, DataType deltaType)
    {
        if (deltaType instanceof BinaryType) {
            return VARBINARY;
        }
        else if (deltaType instanceof BooleanType) {
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
            return TIMESTAMP;
        }

        throw new PrestoException(DELTA_UNSUPPORTED_COLUMN_TYPE,
                format("Column '%s' in Delta table %s contains unsupported data type: %s",
                        columnName,
                        tableName,
                        deltaType.getCatalogString()));
    }
}
