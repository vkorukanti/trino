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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class DeltaSplit
        implements ConnectorSplit
{
    private final String connectorId;
    private final String schema;
    private final String table;
    private final WrappedDeltaCoreTask task;

    @JsonCreator
    public DeltaSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("schemaName") String schema,
            @JsonProperty("tableName") String table,
            @JsonProperty("task") WrappedDeltaCoreTask task)
    {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.schema = requireNonNull(schema, "schema name is null");
        this.table = requireNonNull(table, "table name is null");
        this.task = requireNonNull(task, "task name is null");
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    @JsonProperty
    public String getTable()
    {
        return table;
    }

    @JsonProperty
    public WrappedDeltaCoreTask getTask()
    {
        return task;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return ImmutableList.of();
    }

    @Override
    public Object getInfo()
    {
        return this;
    }
}
