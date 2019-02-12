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

import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.pinot.PinotColumnHandle.PinotColumnType.REGULAR;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class PinotPageSourceTest
{
    final List<PinotColumnHandle> columnHandles = new ArrayList<>();
    PinotPageSource pinotPageSource;
    PinotSplit split;

    @BeforeTest
    void init()
    {
        columnHandles.add(new PinotColumnHandle("varchar", VARCHAR, REGULAR));
        columnHandles.add(new PinotColumnHandle("int", INTEGER, REGULAR));
        columnHandles.add(new PinotColumnHandle("secondsSinceEpoch", BIGINT, REGULAR));
        columnHandles.add(new PinotColumnHandle("boolean", BOOLEAN, REGULAR));
        columnHandles.add(new PinotColumnHandle("double", DOUBLE, REGULAR));
        split = new PinotSplit(
                "pinot",
                Optional.of("table"),
                Optional.of("host"),
                Optional.of("segment"),
                Optional.of("secondsSinceEpoch > 10000"),
                Optional.of("int > 3"),
                Optional.of(10L),
                Optional.empty());
    }

    @Test
    void testShouldPushAggregation()
    {
//        aggregations.put("secondsSinceEpoch", Arrays.asList("max"));
//        pinotPageSource = new PinotPageSource(new PinotConfig(), new PinotScatterGatherQueryClient(new PinotConfig()), split, columnHandles);
//        Assert.assertTrue(pinotPageSource.shouldPushAggregation(aggregations));
//        aggregations.put("secondsSinceEpoch", Arrays.asList("max", "min"));
//        Assert.assertFalse(pinotPageSource.shouldPushAggregation(aggregations));
    }
}
