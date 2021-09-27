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

import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.Row;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.type.TypeManager;

import javax.inject.Inject;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class DeltaPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final TrinoFileSystemFactory fileSystemFactory;
    private final HdfsEnvironment hdfsEnvironment;
    private final TypeManager typeManager;

    @Inject
    public DeltaPageSourceProvider(
            TrinoFileSystemFactory fileSystemFactory,
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        DeltaSplit deltaSplit = (DeltaSplit) split;

        List<DeltaColumnHandle> deltaColumnHandles = columns.stream()
                .map(DeltaColumnHandle.class::cast)
                .collect(Collectors.toList());

        List<DeltaColumnHandle> regularColumnHandles = deltaColumnHandles;
        TableClient tableClient = DeltaClient.createTableClient(
                hdfsEnvironment,
                session,
                fileSystemFactory,
                typeManager,
                deltaSplit.getTableLocation());
        Row scanState = DeltaRowSerDe.convertJSONToRow(tableClient, deltaSplit.getScanStateJson());
        Row scanFile = DeltaRowSerDe.convertJSONToRow(tableClient, deltaSplit.getScanFileJson());
        return new DeltaScanRowPageSource(tableClient, scanState, scanFile, regularColumnHandles);
    }
}
