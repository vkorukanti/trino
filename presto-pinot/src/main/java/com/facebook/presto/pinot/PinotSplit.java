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

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.pipeline.TableScanPipeline;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class PinotSplit
        implements ConnectorSplit
{
    private final String connectorId;
    private final Optional<String> tableName;
    private final Optional<String> host;
    private final Optional<String> segment;
    private final Optional<String> timeFilter;
    private final Optional<String> pinotFilter;
    private final Optional<Long> limit;
    private final Optional<TableScanPipeline> scanPipeline;

    @JsonCreator
    public PinotSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("tableName") Optional<String> tableName,
            @JsonProperty("host") Optional<String> host,
            @JsonProperty("segment") Optional<String> segment,
            @JsonProperty("timeFilter") Optional<String> timeFilter,
            @JsonProperty("pinotFilter") Optional<String> pinotFilter,
            @JsonProperty("limit") Optional<Long> limit,
            @JsonProperty("scanPipeline") Optional<TableScanPipeline> scanPipeline)
    {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.tableName = requireNonNull(tableName, "table name is null");
        this.host = requireNonNull(host, "host is null");
        this.segment = requireNonNull(segment, "segment is null");
        this.pinotFilter = pinotFilter;
        this.timeFilter = timeFilter;
        this.limit = limit;
        this.scanPipeline = requireNonNull(scanPipeline, "scanPipeline is null");
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public Optional<String> getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public Optional<String> getHost()
    {
        return host;
    }

    @JsonProperty
    public Optional<String> getSegment()
    {
        return segment;
    }

    @JsonProperty
    public Optional<String> getTimeFilter()
    {
        return timeFilter;
    }

    @JsonProperty
    public Optional<String> getPinotFilter()
    {
        return pinotFilter;
    }

    @JsonProperty
    public Optional<Long> getLimit()
    {
        return limit;
    }

    @JsonProperty
    public Optional<TableScanPipeline> getScanPipeline()
    {
        return scanPipeline;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return null;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }
}
