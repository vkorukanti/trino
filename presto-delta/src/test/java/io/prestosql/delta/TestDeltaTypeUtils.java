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

import io.airlift.slice.Slices;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.type.Type;
import org.testng.annotations.Test;

import java.math.BigInteger;

import static io.prestosql.delta.DeltaErrorCode.DELTA_INVALID_PARTITION_VALUE;
import static io.prestosql.delta.DeltaTypeUtils.convertPartitionValue;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.Decimals.encodeUnscaledValue;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestDeltaTypeUtils
{
    @Test
    public void partitionValueParsing()
    {
        assertPartitionValue("str", VARCHAR, Slices.utf8Slice("str"));
        assertPartitionValue("3", TINYINT, 3L);
        assertPartitionValue("344", SMALLINT, 344L);
        assertPartitionValue("323243", INTEGER, 323243L);
        assertPartitionValue("234234234233", BIGINT, 234234234233L);
        assertPartitionValue("3.234234", REAL, 1078918577L);
        assertPartitionValue("34534.23423423423", DOUBLE, 34534.23423423423);
        assertPartitionValue("2021-11-18", DATE, 18949L);
        assertPartitionValue("2021-11-18 05:23:43", TIMESTAMP, 1637213023000L);
        assertPartitionValue("true", BOOLEAN, true);
        assertPartitionValue("faLse", BOOLEAN, false);
        assertPartitionValue("234.5", createDecimalType(6, 3), 2345L);
        assertPartitionValue("12345678901234567890123.5", createDecimalType(23, 1), encodeUnscaledValue(new BigInteger("123456789012345678901235")));

        invalidPartitionValue("sdfsdf", BOOLEAN);
        invalidPartitionValue("sdfsdf", DATE);
        invalidPartitionValue("sdfsdf", TIMESTAMP);
    }

    private void assertPartitionValue(String value, Type type, Object expected)
    {
        Object actual = convertPartitionValue("p1", value, type.getTypeSignature());
        assertEquals(actual, expected);
    }

    private void invalidPartitionValue(String value, Type type)
    {
        try {
            convertPartitionValue("p1", value, type.getTypeSignature());
            fail("expected to fail");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), DELTA_INVALID_PARTITION_VALUE.toErrorCode());
            assertTrue(e.getMessage().matches("Can not parse partition value .* of type .* for partition column 'p1'"));
        }
    }
}
