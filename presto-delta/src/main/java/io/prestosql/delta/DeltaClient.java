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

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.Metadata;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HdfsEnvironment.HdfsContext;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.TypeSignature;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static io.prestosql.delta.DeltaErrorCode.DELTA_UNSUPPORTED_DATA_FORMAT;
import static io.prestosql.delta.DeltaTypeUtils.convertDeltaDataTypePrestoDataType;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spi.StandardErrorCode.NOT_FOUND;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Class to interact with Delta lake table APIs.
 */
public class DeltaClient
{
    private final HdfsEnvironment hdfsEnvironment;

    @Inject
    public DeltaClient(HdfsEnvironment hdfsEnvironment)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
    }

    /**
     * Load the delta table.
     *
     * @param session                     Current user session
     * @param schemaTableName             Schema and table name referred to as in the query
     * @param tableLocation               Location of the Delta table on storage
     * @param snapshotId                  Id of the snapshot to read from the Delta table
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
        Optional<DeltaLog> deltaLog = loadDeltaTableLog(session, new Path(tableLocation));
        if (!deltaLog.isPresent()) {
            return Optional.empty();
        }

        // Fetch the snapshot info for given snapshot version. If no snapshot version is given, get the latest snapshot info.
        // Lock the snapshot version here and use it later in the rest of the query (such as fetching file list etc.).
        // If we don't lock the snapshot version here, the query may end up with schema from one version and data files from another
        // version when the underlying delta table is changing while the query is running.
        Snapshot snapshot;
        if (snapshotId.isPresent()) {
            try {
                snapshot = deltaLog.get().getSnapshotForVersionAsOf(snapshotId.get());
            }
            catch (IllegalArgumentException iae) {
                throw new PrestoException(
                        NOT_FOUND,
                        format("Snapshot version %d does not exist in Delta table '%s'.", snapshotId.get(), schemaTableName),
                        iae);
            }
        }
        else if (snapshotAsOfTimestampMillis.isPresent()) {
            try {
                snapshot = deltaLog.get().getSnapshotForTimestampAsOf(snapshotAsOfTimestampMillis.get());
            }
            catch (IllegalArgumentException iae) {
                throw new PrestoException(
                        NOT_FOUND,
                        format(
                                "There is no snapshot exists in Delta table '%s' that is created on or before '%s'",
                                schemaTableName,
                                Instant.ofEpochMilli(snapshotAsOfTimestampMillis.get())),
                        iae);
            }
        }
        else {
            snapshot = deltaLog.get().snapshot();
        }

        Metadata metadata = snapshot.getMetadata();
        String format = metadata.getFormat().getProvider();
        if (!"parquet".equalsIgnoreCase(format)) {
            throw new PrestoException(DELTA_UNSUPPORTED_DATA_FORMAT,
                    format("Delta table %s has unsupported data format: %s. Currently only Parquet data format is supported", schemaTableName, format));
        }

        return Optional.of(new DeltaTable(
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                tableLocation,
                Optional.of(snapshot.getVersion()), // lock the snapshot version
                getSchema(schemaTableName, metadata)));
    }

    /**
     * Get the list of files corresponding to the given Delta table.
     *
     * @return
     */
    public Iterator<AddFile> listFiles(ConnectorSession session, DeltaTable deltaTable)
    {
        Optional<DeltaLog> deltaLog = loadDeltaTableLog(session, new Path(deltaTable.getTableLocation()));
        if (!deltaLog.isPresent()) {
            throw new PrestoException(NOT_FOUND,
                    format("Delta table (%s.%s) no longer exists.", deltaTable.getSchemaName(), deltaTable.getTableName()));
        }

        return deltaLog.get()
                .getSnapshotForVersionAsOf(deltaTable.getSnapshotId().get())
                .getAllFiles()
                .iterator();
    }

    private Optional<DeltaLog> loadDeltaTableLog(ConnectorSession session, Path tableLocation)
    {
        try {
            HdfsContext hdfsContext = new HdfsContext(session.getIdentity());
            Configuration hdfsConf = hdfsEnvironment.getConfiguration(hdfsContext, tableLocation);
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(hdfsContext, tableLocation);
            if (!fileSystem.isDirectory(tableLocation)) {
                Optional.empty();
            }
            return Optional.of(DeltaLog.forTable(hdfsConf, tableLocation));
        }
        catch (IOException io) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Failed to load Delta table: " + io.getMessage(), io);
        }
    }

    /**
     * Utility method that returns the columns in given Delta metadata. Returned columns include regular and partition types.
     * Data type from Delta is mapped to appropriate Presto data type.
     */
    private List<DeltaColumn> getSchema(SchemaTableName tableName, Metadata metadata)
    {
        Set<String> partitionColumns = metadata.getPartitionColumns().stream()
                .map(String::toLowerCase)
                .collect(Collectors.toSet());

        return Arrays.stream(metadata.getSchema().getFields())
                .map(field -> {
                    String columnName = field.getName().toLowerCase(Locale.US);
                    TypeSignature prestoType = convertDeltaDataTypePrestoDataType(tableName, columnName, field.getDataType());
                    return new DeltaColumn(
                            columnName,
                            prestoType,
                            field.isNullable(),
                            partitionColumns.contains(columnName));
                }).collect(Collectors.toList());
    }
}
