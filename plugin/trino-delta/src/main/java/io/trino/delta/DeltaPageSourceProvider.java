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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.ReaderPageSource;
import io.trino.plugin.hive.parquet.ParquetPageSourceFactory;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.Utils;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.MessageType;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.trino.delta.DeltaColumnHandle.ColumnType.PARTITION;
import static io.trino.delta.DeltaTypeUtils.convertPartitionValue;
import static io.trino.parquet.ParquetTypeUtils.getParquetTypeByName;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

public class DeltaPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final TrinoFileSystemFactory fileSystemFactory;
    private final HdfsEnvironment hdfsEnvironment;
    private final TypeManager typeManager;
    private final FileFormatDataSourceStats fileFormatDataSourceStats;

    @Inject
    public DeltaPageSourceProvider(
            TrinoFileSystemFactory fileSystemFactory,
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            FileFormatDataSourceStats fileFormatDataSourceStats)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.fileFormatDataSourceStats = requireNonNull(fileFormatDataSourceStats, "fileFormatDataSourceStats is null");
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
        DeltaTableHandle deltaTableHandle = (DeltaTableHandle) table;

        HdfsContext hdfsContext = new HdfsContext(session.getIdentity());
        Path filePath = new Path(deltaSplit.getFilePath());
        List<DeltaColumnHandle> deltaColumnHandles = columns.stream()
                .map(DeltaColumnHandle.class::cast)
                .collect(Collectors.toList());

        List<DeltaColumnHandle> regularColumnHandles = deltaColumnHandles.stream()
                .filter(columnHandle -> columnHandle.getColumnType() != PARTITION)
                .collect(Collectors.toList());

        TrinoInputFile inputFile = fileSystemFactory.create(session).newInputFile(
                deltaSplit.getFilePath(),
                deltaSplit.getFileSize());

        ReaderPageSource dataPageSource = createParquetPageSource(
                inputFile,
                deltaSplit.getStart(),
                deltaSplit.getLength(),
                regularColumnHandles,
                typeManager,
                deltaTableHandle.getPredicate(),
                fileFormatDataSourceStats);

        return new DeltaPageSource(
                deltaColumnHandles,
                convertPartitionValues(deltaColumnHandles, deltaSplit.getPartitionValues()),
                dataPageSource.get());
    }

    /**
     * Go through all the output columns, identify the partition columns and convert the partition values to Trino internal format.
     */
    private Map<String, Block> convertPartitionValues(
            List<DeltaColumnHandle> allColumns,
            Map<String, String> partitionValues)
    {
        return allColumns.stream()
                .filter(columnHandle -> columnHandle.getColumnType() == PARTITION)
                .collect(toMap(
                        DeltaColumnHandle::getName,
                        columnHandle -> {
                                Type columnType = typeManager.getType(columnHandle.getDataType());
                                return Utils.nativeValueToBlock(
                                        columnType,
                                        convertPartitionValue(
                                                columnHandle.getName(),
                                                partitionValues.get(columnHandle.getName()),
                                                columnType));
                        }));
    }

    private static ReaderPageSource createParquetPageSource(
            TrinoInputFile inputFile,
            long start,
            long length,
            List<DeltaColumnHandle> columns,
            TypeManager typeManager,
            TupleDomain<DeltaColumnHandle> effectivePredicate,
            FileFormatDataSourceStats stats)
    {
        ParquetReaderOptions options = new ParquetReaderOptions();
        ImmutableSet.Builder<String> missingColumnNames = ImmutableSet.builder();
        ImmutableList.Builder<HiveColumnHandle> hiveColumnHandles = ImmutableList.builder();
        for (DeltaColumnHandle column : columns) {
            toHiveColumnHandle(column, typeManager).ifPresentOrElse(
                    hiveColumnHandles::add,
                    () -> missingColumnNames.add(column.getName()));
        }

        TupleDomain<HiveColumnHandle> parquetPredicate =
                getParquetTupleDomain(effectivePredicate, typeManager);

        return ParquetPageSourceFactory.createPageSource(
                inputFile,
                start,
                length,
                hiveColumnHandles.build(),
                parquetPredicate,
                true,
                DateTimeZone.getDefault(),
                stats,
                options,
                Optional.empty(),
                100);
    }

    public static TupleDomain<HiveColumnHandle> getParquetTupleDomain(
            TupleDomain<DeltaColumnHandle> effectivePredicate,
            TypeManager typeManager)
    {
        if (effectivePredicate.isNone()) {
            return TupleDomain.none();
        }

        ImmutableMap.Builder<HiveColumnHandle, Domain> predicate = ImmutableMap.builder();
        effectivePredicate.getDomains().get().forEach((columnHandle, domain) -> {
            String baseType = columnHandle.getDataType().getBase();
            // skip looking up predicates for complex types as Parquet only stores stats for primitives
            if (!baseType.equals(StandardTypes.MAP) && !baseType.equals(StandardTypes.ARRAY) && !baseType.equals(StandardTypes.ROW)) {
                Optional<HiveColumnHandle> hiveColumnHandle =
                        toHiveColumnHandle(columnHandle, typeManager);
                hiveColumnHandle.ifPresent(column -> predicate.put(column, domain));
            }
        });
        return TupleDomain.withColumnDomains(predicate.buildOrThrow());
    }

    public static TupleDomain<ColumnDescriptor> getParquetTupleDomain(Map<List<String>, ColumnDescriptor> descriptorsByPath, TupleDomain<DeltaColumnHandle> effectivePredicate)
    {
        if (effectivePredicate.isNone()) {
            return TupleDomain.none();
        }

        ImmutableMap.Builder<ColumnDescriptor, Domain> predicate = ImmutableMap.builder();
        for (Map.Entry<DeltaColumnHandle, Domain> entry : effectivePredicate.getDomains().get().entrySet()) {
            DeltaColumnHandle columnHandle = entry.getKey();

            ColumnDescriptor descriptor = descriptorsByPath.get(ImmutableList.of(columnHandle.getName()));
            if (descriptor != null) {
                predicate.put(descriptor, entry.getValue());
            }
        }
        return TupleDomain.withColumnDomains(predicate.buildOrThrow());
    }

    public static Optional<org.apache.parquet.schema.Type> getParquetType(
            MessageType messageType,
            DeltaColumnHandle column)
    {
        org.apache.parquet.schema.Type type = getParquetTypeByName(column.getName(), messageType);
        return Optional.of(type);
    }

    public static Optional<HiveColumnHandle> toHiveColumnHandle(
            DeltaColumnHandle deltaLakeColumnHandle,
            TypeManager typeManager)
    {
        return Optional.of(deltaLakeColumnHandle.toHiveColumnHandle(typeManager));
    }
}
