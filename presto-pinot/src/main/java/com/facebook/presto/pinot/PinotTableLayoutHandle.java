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

import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.pipeline.TableScanPipeline;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;

public class PinotTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final PinotTableHandle table;
    private final Optional<TableScanPipeline> scanPipeline;

    @JsonCreator
    public PinotTableLayoutHandle(
            @JsonProperty("table") PinotTableHandle table,
            @JsonProperty("scanPipeline") Optional<TableScanPipeline> scanPipeline)
    {
        this.table = table;
        this.scanPipeline = scanPipeline;
    }

    @JsonProperty
    public PinotTableHandle getTable()
    {
        return table;
    }

    @JsonProperty
    public Optional<TableScanPipeline> getScanPipeline()
    {
        return scanPipeline;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PinotTableLayoutHandle that = (PinotTableLayoutHandle) o;
        return Objects.equals(table, that.table) && Objects.equals(scanPipeline, that.scanPipeline);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(table, scanPipeline);
    }

    @Override
    public String toString()
    {
        StringBuilder result = new StringBuilder();
        result.append(table.toString());
        if (scanPipeline.isPresent()) {
            result.append(", pql=").append(PinotPushdownQueryGenerator.generate(scanPipeline.get()));
            result.append(", scanPipeline=").append(scanPipeline.get().toString());
        }
        return result.toString();
    }
}
