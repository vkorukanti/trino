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

import io.airlift.json.JsonCodec;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.TableNotFoundException;
import io.delta.kernel.defaults.client.DefaultTableClient;
import io.delta.kernel.types.StructType;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.deltalake.DataFileInfo;
import io.trino.plugin.deltalake.DeltaLakeMergeResult;
import io.trino.plugin.deltalake.DeltaLakeMetadata;
import io.trino.plugin.deltalake.DeltaLakeRedirectionsProvider;
import io.trino.plugin.deltalake.DeltaLakeTableName;
import io.trino.plugin.deltalake.LocatedTableHandle;
import io.trino.plugin.deltalake.metastore.DeltaLakeMetastore;
import io.trino.plugin.deltalake.metastore.DeltaMetastoreTable;
import io.trino.plugin.deltalake.statistics.CachingExtendedStatisticsAccess;
import io.trino.plugin.deltalake.statistics.DeltaLakeTableStatisticsProvider;
import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointWriterManager;
import io.trino.plugin.deltalake.transactionlog.writer.TransactionLogWriterFactory;
import io.trino.plugin.hive.TrinoViewHiveMetastore;
import io.trino.plugin.hive.security.AccessControlMetadata;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;
public class KernelDeltaLakeMetadata
        extends DeltaLakeMetadata
{
    private static final Map<SnapshotKey, Snapshot> snapshotCache = new ConcurrentHashMap<>();
    private DefaultTableClient tableClient = DefaultTableClient.create(new Configuration());

    public KernelDeltaLakeMetadata(DeltaLakeMetastore metastore,
            TransactionLogAccess transactionLogAccess,
            DeltaLakeTableStatisticsProvider tableStatisticsProvider,
            TrinoFileSystemFactory fileSystemFactory,
            TypeManager typeManager,
            AccessControlMetadata accessControlMetadata,
            TrinoViewHiveMetastore trinoViewHiveMetastore,
            int domainCompactionThreshold, boolean unsafeWritesEnabled,
            JsonCodec<DataFileInfo> dataFileInfoCodec,
            JsonCodec<DeltaLakeMergeResult> mergeResultJsonCodec,
            TransactionLogWriterFactory transactionLogWriterFactory,
            NodeManager nodeManager,
            CheckpointWriterManager checkpointWriterManager,
            long defaultCheckpointInterval,
            boolean deleteSchemaLocationsFallback,
            DeltaLakeRedirectionsProvider deltaLakeRedirectionsProvider,
            CachingExtendedStatisticsAccess statisticsAccess,
            boolean useUniqueTableLocation,
            boolean allowManagedTableRename)
    {
        super(
                metastore,
                transactionLogAccess,
                tableStatisticsProvider,
                fileSystemFactory,
                typeManager,
                accessControlMetadata,
                trinoViewHiveMetastore,
                domainCompactionThreshold,
                unsafeWritesEnabled,
                dataFileInfoCodec,
                mergeResultJsonCodec,
                transactionLogWriterFactory,
                nodeManager,
                checkpointWriterManager,
                defaultCheckpointInterval,
                deleteSchemaLocationsFallback,
                deltaLakeRedirectionsProvider,
                statisticsAccess,
                useUniqueTableLocation,
                allowManagedTableRename);
    }

    @Override
    public LocatedTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");
        if (!DeltaLakeTableName.isDataTable(tableName.getTableName())) {
            // Pretend the table does not exist to produce better error message in case of table redirects to Hive
            return null;
        }
        Optional<DeltaMetastoreTable> table = getMetastore()
                .getTable(tableName.getSchemaName(), tableName.getTableName());

        if (table.isEmpty()) {
            return null;
        }
        boolean managed = table.get().managed();
        String tableLocation = table.get().location();

        try {
            Table deltaTable = Table.forPath(tableClient, tableLocation);

            Snapshot snapshot = deltaTable.getLatestSnapshot(tableClient);
            long version = snapshot.getVersion(tableClient);

            SnapshotKey snapshotKey = new SnapshotKey(tableLocation, version);
            snapshotCache.put(snapshotKey, snapshot);
            return new KernelDeltaLakeTableHandle(
                    tableName.getSchemaName(),
                    tableName.getTableName(),
                    managed,
                    tableLocation,
                    version);
        } catch (TableNotFoundException tbne) {
            return null;
        }
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        return super.getTableMetadata(session, table);
    }

    private static class SnapshotKey {
        private final String tableLocation;
        private final long snapshotVersion;

        public SnapshotKey(String tableLocation, long snapshotVersion)
        {
            this.tableLocation = tableLocation;
            this.snapshotVersion = snapshotVersion;
        }

        public String getTableLocation()
        {
            return tableLocation;
        }

        public long getSnapshotVersion()
        {
            return snapshotVersion;
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
            SnapshotKey that = (SnapshotKey) o;
            return snapshotVersion == that.snapshotVersion && Objects.equals(tableLocation, that.tableLocation);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(tableLocation, snapshotVersion);
        }
    }
}
