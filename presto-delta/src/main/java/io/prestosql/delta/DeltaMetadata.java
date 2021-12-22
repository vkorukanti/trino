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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.plugin.hive.metastore.Storage;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableLayout;
import io.prestosql.spi.connector.ConnectorTableLayoutHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTableProperties;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.ConstraintApplicationResult;
import io.prestosql.spi.connector.ProjectionApplicationResult;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SchemaTablePrefix;
import io.prestosql.spi.expression.ConnectorExpression;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.TypeManager;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.delta.DeltaColumnHandle.ColumnType.PARTITION;
import static io.prestosql.delta.DeltaColumnHandle.ColumnType.REGULAR;
import static io.prestosql.delta.DeltaExpressionUtils.splitPredicate;
import static io.prestosql.delta.DeltaSessionProperties.isFilterPushdownEnabled;
import static io.prestosql.delta.DeltaSessionProperties.isProjectionPushdownEnabled;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

public class DeltaMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(DeltaMetadata.class);

    /**
     * Special schema used when querying a Delta table by storage location.
     * Ex. SELECT * FROM delta."$PATH$"."s3://bucket/path/to/table". User is not able to list any tables
     * in this schema. It is just used to query a Delta table by storage location.
     */
    private static final String PATH_SCHEMA = "$PATH$";

    private final String connectorId;
    private final DeltaClient deltaClient;
    private final HiveMetastore metastore;
    private final TypeManager typeManager;
    private final DeltaConfig config;

    @Inject
    public DeltaMetadata(
            DeltaConnectorId connectorId,
            DeltaClient deltaClient,
            HiveMetastore metastore,
            TypeManager typeManager,
            DeltaConfig config)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.deltaClient = requireNonNull(deltaClient, "deltaClient is null");
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.config = requireNonNull(config, "config is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        ArrayList<String> schemas = new ArrayList<>();
        schemas.addAll(metastore.getAllDatabases());
        schemas.add(PATH_SCHEMA.toLowerCase(Locale.ROOT));
        return schemas;
    }

    @Override
    public DeltaTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();
        if (!listSchemaNames(session).contains(schemaName)) {
            return null; // indicates table doesn't exist
        }

        DeltaTableName deltaTableName = DeltaTableName.from(tableName);
        String tableLocation;
        if (PATH_SCHEMA.equalsIgnoreCase(schemaName)) {
            tableLocation = deltaTableName.getTableNameOrPath();
        }
        else {
            Optional<Table> metastoreTable = metastore.getTable(
                    new HiveIdentity(session),
                    schemaName,
                    deltaTableName.getTableNameOrPath());
            if (!metastoreTable.isPresent()) {
                return null; // indicates table doesn't exist
            }

            Map<String, String> tableParameters = metastoreTable.get().getParameters();
            Storage storage = metastoreTable.get().getStorage();
            tableLocation = storage.getLocation();

            // Delta table written using Spark and Hive have set the table parameter
            // "spark.sql.sources.provider = delta". If this property is found table
            // location is found in SerDe properties with key "path".
            if ("delta".equalsIgnoreCase(tableParameters.get("spark.sql.sources.provider"))) {
                tableLocation = storage.getSerdeParameters().get("path");
                if (Strings.isNullOrEmpty(tableLocation)) {
                    log.warn("Location key ('path') is missing in SerDe properties for table %s. " +
                            "Using the 'location' attribute as the table location.", schemaTableName);
                    // fallback to using the location attribute
                    tableLocation = storage.getLocation();
                }
            }
        }

        Optional<DeltaTable> table = deltaClient.getTable(
                session,
                schemaTableName,
                tableLocation,
                deltaTableName.getSnapshotId(),
                deltaTableName.getSnapshotAsOfTimestamp());
        if (table.isPresent()) {
            return new DeltaTableHandle(connectorId, table.get(), TupleDomain.all(), Optional.empty());
        }
        return null;
    }

    @Override
    public boolean usesLegacyTableLayouts()
    {
        return false;
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return new ConnectorTableProperties();
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle handle, Constraint constraint)
    {
        if (!isFilterPushdownEnabled(session)) {
            return Optional.empty();
        }

        DeltaTableHandle table = (DeltaTableHandle) handle;
        if (constraint.getSummary().equals(table.getPredicate())) {
            return Optional.empty();
        }

        // Split the predicate into partition column predicate and other column predicates
        // Only the partition column predicate is fully enforced. Other predicate is partially enforced (best effort).
        List<TupleDomain<ColumnHandle>> predicate = splitPredicate(constraint.getSummary());
        TupleDomain<ColumnHandle> unenforcedPredicate = predicate.get(1);

        DeltaTableHandle newDeltaTableHandle = table.withPredicate(
                constraint.getSummary().transform(DeltaColumnHandle.class::cast),
                constraint.getSummary().toString(session));

        return Optional.of(new ConstraintApplicationResult<>(
                newDeltaTableHandle,
                unenforcedPredicate));
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
            ConnectorSession session, ConnectorTableHandle handle,
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments)
    {
        if (!isProjectionPushdownEnabled(session)) {
            return Optional.empty();
        }

        // TODO: implement
        return Optional.empty();
    }

    @Override
    public ConnectorTableLayout getTableLayout(ConnectorSession session, ConnectorTableLayoutHandle handle)
    {
        return new ConnectorTableLayout(handle);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        DeltaTableHandle deltaTableHandle = (DeltaTableHandle) table;
        checkConnectorId(deltaTableHandle);
        return getTableMetadata(session, deltaTableHandle.toSchemaTableName());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        List<String> schemaNames = schemaName.isEmpty() ? listSchemaNames(session) : ImmutableList.of(schemaName.get());

        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        for (String schema : schemaNames) {
            for (String table : metastore.getAllTables(schema)) {
                tableNames.add(new SchemaTableName(schema, table));
            }
        }
        return tableNames.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        DeltaTableHandle deltaTableHandle = (DeltaTableHandle) tableHandle;
        checkConnectorId(deltaTableHandle);

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        for (DeltaColumn column : deltaTableHandle.getDeltaTable().getColumns()) {
            columnHandles.put(
                    column.getName(),
                    new DeltaColumnHandle(
                            connectorId,
                            column.getName(),
                            column.getType(),
                            column.isPartition() ? PARTITION : REGULAR));
        }
        return columnHandles.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(session, tableName);
            // table can disappear during listing operation
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.build();
    }

    private ConnectorTableMetadata getTableMetadata(ConnectorSession session, SchemaTableName tableName)
    {
        DeltaTableHandle tableHandle = getTableHandle(session, tableName);
        if (tableHandle == null) {
            return null;
        }

        List<ColumnMetadata> columnMetadata = tableHandle.getDeltaTable().getColumns().stream()
                .map(column -> getColumnMetadata(column))
                .collect(Collectors.toList());

        return new ConnectorTableMetadata(tableName, columnMetadata);
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return getColumnMetadata(columnHandle);
    }

    private ColumnMetadata getColumnMetadata(ColumnHandle columnHandle)
    {
        DeltaColumnHandle deltaColumnHandle = (DeltaColumnHandle) columnHandle;
        return new ColumnMetadata(deltaColumnHandle.getName(), typeManager.getType(deltaColumnHandle.getDataType()));
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getTable().isEmpty()) {
            return listTables(session, prefix.getSchema());
        }

        return singletonList(new SchemaTableName(prefix.getSchema().get(), prefix.getTable().get()));
    }

    private ColumnMetadata getColumnMetadata(DeltaColumn deltaColumn)
    {
        return new ColumnMetadata(deltaColumn.getName(), typeManager.getType(deltaColumn.getType()));
    }

    private void checkConnectorId(DeltaTableHandle tableHandle)
    {
        checkArgument(tableHandle.getConnectorId().equals(connectorId), "table handle is not for this connector");
    }
}
