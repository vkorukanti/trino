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

import io.delta.kernel.Scan;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.Tuple2;
import io.trino.delta.client.TrinoDeltaTableClient;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;

import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.trino.delta.DeltaTypeUtils.convertDeltaType;
import static java.util.Objects.requireNonNull;

/**
 * Class to interact with Delta lake table APIs.
 */
public class DeltaClient
{
    private final HdfsEnvironment hdfsEnvironment;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final TypeManager typeManager;

    @Inject
    public DeltaClient(HdfsEnvironment hdfsEnvironment, TrinoFileSystemFactory fileSystemFactory, TypeManager typeManager)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    /**
     * Load the delta table.
     *
     * @param session Current user session
     * @param schemaTableName Schema and table name referred to as in the query
     * @param tableLocation Location of the Delta table on storage
     * @param snapshotId Id of the snapshot to read from the Delta table
     * @param snapshotAsOfTimestampMillis Latest snapshot as of given timestamp
     * @return If the table is found return {@link DeltaTable}.
     */
    public Optional<DeltaTable> getTable(
            ConnectorSession session,
            SchemaTableName schemaTableName,
            String tableLocation,
            Optional<Long> snapshotId,
            Optional<Long> snapshotAsOfTimestampMillis)
    {
        if (snapshotId.isPresent() || snapshotAsOfTimestampMillis.isPresent()) {
            throw new UnsupportedOperationException("Reading specific snapshot of the Delta Lake " +
                    "table is not yet supported");
        }

        try {
            TableClient tableClient = createTableClient(
                    hdfsEnvironment,
                    session,
                    fileSystemFactory,
                    typeManager,
                    tableLocation);
            Snapshot snapshot = loadSnapshot(tableClient, tableLocation);

            StructType schema = snapshot.getSchema(tableClient);

            return Optional.of(new DeltaTable(
                    schemaTableName.getSchemaName(),
                    schemaTableName.getTableName(),
                    tableLocation,
                    Optional.of(snapshot.getVersion(tableClient)), // lock the snapshot version
                    getSchema(schemaTableName, schema)));
        }
        catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Get the list of scan file rows as an iterator of {@link ColumnarBatch}
     *
     * @return Iterator of {@link ColumnarBatch} where each correspond to one scan file.
     */
    public Tuple2<Row, CloseableIterator<ColumnarBatch>> getScanStateAndFiles(
            ConnectorSession session,
            DeltaTable deltaTable)
    {
        try {
            TableClient tableClient = createTableClient(
                    hdfsEnvironment,
                    session,
                    fileSystemFactory,
                    typeManager, deltaTable.getTableLocation());
            Snapshot snapshot = loadSnapshot(tableClient, deltaTable.getTableLocation());

            Scan scan = snapshot.getScanBuilder(tableClient).build();

            return new Tuple2<>(scan.getScanState(tableClient), scan.getScanFiles(tableClient));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Snapshot loadSnapshot(TableClient tableClient, String location)
            throws Exception
    {
        return Table.forPath(location)
                .getLatestSnapshot(tableClient);
    }

    /**
     * Utility method that returns the columns in given Delta metadata. Returned columns include
     * regular and partition types. Data type from Delta is mapped to appropriate Trino data type.
     */
    private static List<DeltaColumn> getSchema(SchemaTableName tableName, StructType schema)
    {
        return schema.fields().stream()
                .map(field -> {
                    String columnName = field.getName().toLowerCase(Locale.US);
                    TypeSignature trinoType =
                            convertDeltaType(
                                    tableName,
                                    columnName,
                                    field.getDataType());

                    return new DeltaColumn(
                            columnName,
                            trinoType,
                            field.isNullable(),
                            false /* isPartitionColumn - TODO - may not needed */);
                }).collect(Collectors.toList());
    }

    public static TableClient createTableClient(
            HdfsEnvironment hdfsEnvironment,
            ConnectorSession session,
            TrinoFileSystemFactory trinoFileSystemFactory,
            TypeManager typeManager,
            String tableLocation)
    {
        HdfsContext hdfsContext = new HdfsContext(session);
        Configuration conf = hdfsEnvironment.getConfiguration(hdfsContext, new Path(tableLocation));
        TrinoFileSystem trinoFileSystem = trinoFileSystemFactory.create(session);

        return new TrinoDeltaTableClient(conf, trinoFileSystem, typeManager);
    }
}
