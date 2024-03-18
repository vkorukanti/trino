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
package io.trino.plugin.deltalake.kernel;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.data.DefaultJsonRow;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampType;

import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;

public class KernelRowSerDeUtils
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private KernelRowSerDeUtils()
    {
    }

    /**
     * Utility method to serialize a {@link Row} as a JSON string
     */
    public static String serializeRowToJson(Row row)
    {
        Map<String, Object> rowObject = convertRowToJsonObject(row);
        try {
            Map<String, Object> rowWithSchema = new HashMap<>();
            rowWithSchema.put("schema", row.getSchema().toJson());
            rowWithSchema.put("row", rowObject);
            return OBJECT_MAPPER.writeValueAsString(rowWithSchema);
        }
        catch (JsonProcessingException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Utility method to deserialize a {@link Row} object from the JSON form.
     */
    public static Row deserializeRowFromJson(TableClient tableClient, String jsonRowWithSchema)
    {
        try {
            JsonNode jsonNode = OBJECT_MAPPER.readTree(jsonRowWithSchema);
            JsonNode schemaNode = jsonNode.get("schema");
            StructType schema =
                    tableClient.getJsonHandler().deserializeStructType(schemaNode.asText());
            return parseRowFromJsonWithSchema((ObjectNode) jsonNode.get("row"), schema);
        }
        catch (JsonProcessingException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    private static Map<String, Object> convertRowToJsonObject(Row row)
    {
        StructType rowType = row.getSchema();
        Map<String, Object> rowObject = new HashMap<>();
        for (int fieldId = 0; fieldId < rowType.length(); fieldId++) {
            StructField field = rowType.at(fieldId);
            DataType fieldType = field.getDataType();
            String name = field.getName();

            if (row.isNullAt(fieldId)) {
                rowObject.put(name, null);
                continue;
            }

            Object value;
            if (fieldType instanceof BooleanType) {
                value = row.getBoolean(fieldId);
            }
            else if (fieldType instanceof ByteType) {
                value = row.getByte(fieldId);
            }
            else if (fieldType instanceof ShortType) {
                value = row.getShort(fieldId);
            }
            else if (fieldType instanceof IntegerType) {
                value = row.getInt(fieldId);
            }
            else if (fieldType instanceof LongType) {
                value = row.getLong(fieldId);
            }
            else if (fieldType instanceof FloatType) {
                value = row.getFloat(fieldId);
            }
            else if (fieldType instanceof DoubleType) {
                value = row.getDouble(fieldId);
            }
            else if (fieldType instanceof DateType) {
                value = row.getInt(fieldId);
            }
            else if (fieldType instanceof TimestampType) {
                value = row.getLong(fieldId);
            }
            else if (fieldType instanceof StringType) {
                value = row.getString(fieldId);
            }
            else if (fieldType instanceof ArrayType) {
                value = VectorUtils.toJavaList(row.getArray(fieldId));
            }
            else if (fieldType instanceof MapType) {
                value = VectorUtils.toJavaMap(row.getMap(fieldId));
            }
            else if (fieldType instanceof StructType) {
                Row subRow = row.getStruct(fieldId);
                value = convertRowToJsonObject(subRow);
            }
            else {
                throw new UnsupportedOperationException("NYI");
            }

            rowObject.put(name, value);
        }

        return rowObject;
    }

    private static Row parseRowFromJsonWithSchema(ObjectNode rowJsonNode, StructType rowType)
    {
        return new DefaultJsonRow(rowJsonNode, rowType);
    }
}
