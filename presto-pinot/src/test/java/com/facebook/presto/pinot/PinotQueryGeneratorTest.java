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

import com.facebook.presto.testing.assertions.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.pinot.PinotColumnHandle.PinotColumnType.REGULAR;
import static com.facebook.presto.pinot.PinotQueryGenerator.getPinotQuery;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class PinotQueryGeneratorTest
{
    final List<PinotColumnHandle> columnHandlesNoAgg = new ArrayList<>();
    final List<PinotColumnHandle> columnHandlesWithAgg = new ArrayList<>();

    @BeforeTest
    void init()
    {
        columnHandlesNoAgg.add(new PinotColumnHandle("varchar", VARCHAR, REGULAR));
        columnHandlesNoAgg.add(new PinotColumnHandle("int", INTEGER, REGULAR));
        columnHandlesNoAgg.add(new PinotColumnHandle("secondsSinceEpoch", BIGINT, REGULAR));
        columnHandlesNoAgg.add(new PinotColumnHandle("boolean", BOOLEAN, REGULAR));
        columnHandlesNoAgg.add(new PinotColumnHandle("double", DOUBLE, REGULAR));

        columnHandlesWithAgg.add(new PinotColumnHandle("varchar", VARCHAR, REGULAR));
        columnHandlesWithAgg.add(new PinotColumnHandle("int", INTEGER, REGULAR));
        columnHandlesWithAgg.add(new PinotColumnHandle("boolean", INTEGER, REGULAR));
    }

    @Test
    public void testGetPinotQuerySelectAll()
    {
        String expectedQuery = "SELECT varchar, int, secondsSinceEpoch, boolean, double FROM table  LIMIT 10";
        Assert.assertEquals(expectedQuery, getPinotQuery(new PinotConfig(), columnHandlesNoAgg, "", "", "table", Optional.of(10L)));
    }

    @Test
    public void testGetPinotQueryWithPredicate()
    {
        String expectedQuery = "SELECT varchar, int, secondsSinceEpoch, boolean, double FROM table WHERE ((int > 3)) AND ((secondsSinceEpoch > 10000)) LIMIT 10";
        Assert.assertEquals(expectedQuery, getPinotQuery(new PinotConfig(), columnHandlesNoAgg, "(int > 3)", "(secondsSinceEpoch > 10000)", "table", Optional.of(10L)));
    }
}
