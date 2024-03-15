package io.trino.plugin.deltalake;/*
 * Copyright (2023) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import io.airlift.json.JsonCodec;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.deltalake.metastore.DeltaLakeMetastore;
import io.trino.plugin.deltalake.statistics.CachingExtendedStatisticsAccess;
import io.trino.plugin.deltalake.statistics.DeltaLakeTableStatisticsProvider;
import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.plugin.deltalake.transactionlog.checkpoint.CheckpointWriterManager;
import io.trino.plugin.deltalake.transactionlog.writer.TransactionLogWriterFactory;
import io.trino.plugin.hive.TrinoViewHiveMetastore;
import io.trino.plugin.hive.security.AccessControlMetadata;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TypeManager;

public class KernelDeltaLakeMetadata extends DeltaLakeMetadata {
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
                                   boolean allowManagedTableRename) {
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
    public LocatedTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
        return super.getTableHandle(session, tableName);
    }
}
