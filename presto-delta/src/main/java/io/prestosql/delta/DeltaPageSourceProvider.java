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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.parquet.Field;
import io.prestosql.parquet.ParquetCorruptionException;
import io.prestosql.parquet.ParquetDataSource;
import io.prestosql.parquet.ParquetDataSourceId;
import io.prestosql.parquet.ParquetReaderOptions;
import io.prestosql.parquet.RichColumnDescriptor;
import io.prestosql.parquet.predicate.Predicate;
import io.prestosql.parquet.reader.MetadataReader;
import io.prestosql.parquet.reader.ParquetReader;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HdfsEnvironment.HdfsContext;
import io.prestosql.plugin.hive.parquet.HdfsParquetDataSource;
import io.prestosql.plugin.hive.parquet.ParquetPageSource;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.DynamicFilter;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.predicate.Utils;
import io.prestosql.spi.security.ConnectorIdentity;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.AccessControlException;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;

import javax.inject.Inject;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.nullToEmpty;
import static io.prestosql.delta.DeltaColumnHandle.ColumnType.PARTITION;
import static io.prestosql.delta.DeltaColumnHandle.ColumnType.REGULAR;
import static io.prestosql.delta.DeltaColumnHandle.ColumnType.SUBFIELD;
import static io.prestosql.delta.DeltaErrorCode.DELTA_BAD_DATA;
import static io.prestosql.delta.DeltaErrorCode.DELTA_CANNOT_OPEN_SPLIT;
import static io.prestosql.delta.DeltaErrorCode.DELTA_MISSING_DATA;
import static io.prestosql.delta.DeltaTypeUtils.convertPartitionValue;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.parquet.ParquetTypeUtils.getColumnIO;
import static io.prestosql.parquet.ParquetTypeUtils.getDescriptors;
import static io.prestosql.parquet.ParquetTypeUtils.getParquetTypeByName;
import static io.prestosql.parquet.ParquetTypeUtils.lookupColumnByName;
import static io.prestosql.parquet.predicate.PredicateUtils.buildPredicate;
import static io.prestosql.parquet.predicate.PredicateUtils.predicateMatches;
import static io.prestosql.plugin.hive.parquet.ParquetColumnIOConverter.constructField;
import static io.prestosql.spi.StandardErrorCode.PERMISSION_DENIED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;
import static org.joda.time.DateTimeZone.UTC;

public class DeltaPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final HdfsEnvironment hdfsEnvironment;
    private final TypeManager typeManager;
    private final FileFormatDataSourceStats fileFormatDataSourceStats;

    @Inject
    public DeltaPageSourceProvider(
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            FileFormatDataSourceStats fileFormatDataSourceStats)
    {
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

        ConnectorPageSource dataPageSource = createParquetPageSource(
                hdfsEnvironment,
                session.getIdentity(),
                hdfsEnvironment.getConfiguration(hdfsContext, filePath),
                filePath,
                deltaSplit.getStart(),
                deltaSplit.getLength(),
                deltaSplit.getFileSize(),
                regularColumnHandles,
                typeManager,
                deltaTableHandle.getPredicate(),
                fileFormatDataSourceStats);

        return new DeltaPageSource(
                deltaColumnHandles,
                convertPartitionValues(deltaColumnHandles, deltaSplit.getPartitionValues()),
                dataPageSource);
    }

    /**
     * Go through all the output columns, identify the partition columns and convert the partition values to Presto internal format.
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

    private static ConnectorPageSource createParquetPageSource(
            HdfsEnvironment hdfsEnvironment,
            ConnectorIdentity identity,
            Configuration configuration,
            Path path,
            long start,
            long length,
            long fileSize,
            List<DeltaColumnHandle> columns,
            TypeManager typeManager,
            TupleDomain<DeltaColumnHandle> effectivePredicate,
            FileFormatDataSourceStats stats)
    {
        AggregatedMemoryContext systemMemoryContext = newSimpleAggregatedMemoryContext();

        ParquetDataSource dataSource = null;
        try {
            FSDataInputStream inputStream = hdfsEnvironment
                    .getFileSystem(identity.getUser(), path, configuration)
                    .open(path);
            ParquetReaderOptions readerOptions = new ParquetReaderOptions();
            dataSource = new HdfsParquetDataSource(new ParquetDataSourceId(path.toString()), fileSize, inputStream, stats, readerOptions);
            ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource);

            FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
            MessageType fileSchema = fileMetaData.getSchema();

            Optional<MessageType> message = columns.stream()
                    .filter(column -> column.getColumnType() == REGULAR)
                    .map(column -> getParquetType(fileSchema, column))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .map(type -> new MessageType(fileSchema.getName(), type))
                    .reduce(MessageType::union);

            MessageType requestedSchema = message
                    .orElse(new MessageType(fileSchema.getName(), ImmutableList.of()));

            ImmutableList.Builder<BlockMetaData> footerBlocks = ImmutableList.builder();
            for (BlockMetaData block : parquetMetadata.getBlocks()) {
                long firstDataPage = block.getColumns().get(0).getFirstDataPageOffset();
                if (firstDataPage >= start && firstDataPage < start + length) {
                    footerBlocks.add(block);
                }
            }

            Map<List<String>, RichColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, requestedSchema);
            TupleDomain<ColumnDescriptor> parquetTupleDomain = getParquetTupleDomain(descriptorsByPath, effectivePredicate);
            Predicate parquetPredicate = buildPredicate(requestedSchema, parquetTupleDomain, descriptorsByPath, UTC);
            final ParquetDataSource finalDataSource = dataSource;
            ImmutableList.Builder<BlockMetaData> blocks = ImmutableList.builder();
            for (BlockMetaData block : footerBlocks.build()) {
                if (predicateMatches(
                        parquetPredicate,
                        block,
                        finalDataSource,
                        descriptorsByPath,
                        parquetTupleDomain)) {
                    blocks.add(block);
                }
            }
            MessageColumnIO messageColumnIO = getColumnIO(fileSchema, requestedSchema);
            ParquetReader parquetReader = new ParquetReader(
                    Optional.ofNullable(fileMetaData.getCreatedBy()),
                    messageColumnIO,
                    blocks.build(),
                    dataSource,
                    UTC,
                    systemMemoryContext,
                    readerOptions);

            ImmutableList.Builder<String> namesBuilder = ImmutableList.builder();
            ImmutableList.Builder<Type> typesBuilder = ImmutableList.builder();
            ImmutableList.Builder<Optional<Field>> fieldsBuilder = ImmutableList.builder();
            for (DeltaColumnHandle column : columns) {
                checkArgument(column.getColumnType() == REGULAR || column.getColumnType() == SUBFIELD,
                        "column type must be regular or subfield column");

                String name = column.getName();
                Type type = typeManager.getType(column.getDataType());

                namesBuilder.add(name);
                typesBuilder.add(type);

                if (getParquetType(fileSchema, column).isPresent()) {
                    String columnName = name;
                    fieldsBuilder.add(constructField(type, lookupColumnByName(messageColumnIO, columnName)));
                }
                else {
                    fieldsBuilder.add(Optional.empty());
                }
            }
            return new ParquetPageSource(
                    parquetReader,
                    typesBuilder.build(),
                    fieldsBuilder.build());
        }
        catch (Exception e) {
            try {
                if (dataSource != null) {
                    dataSource.close();
                }
            }
            catch (IOException ignored) {
            }
            if (e instanceof PrestoException) {
                throw (PrestoException) e;
            }
            if (e instanceof ParquetCorruptionException) {
                throw new PrestoException(DELTA_BAD_DATA, e);
            }
            if (e instanceof AccessControlException) {
                throw new PrestoException(PERMISSION_DENIED, e.getMessage(), e);
            }
            if (nullToEmpty(e.getMessage()).trim().equals("Filesystem closed") || e instanceof FileNotFoundException) {
                throw new PrestoException(DELTA_CANNOT_OPEN_SPLIT, e);
            }
            String message = format("Error opening Hive split %s (offset=%s, length=%s): %s", path, start, length, e.getMessage());
            if (e.getClass().getSimpleName().equals("BlockMissingException")) {
                throw new PrestoException(DELTA_MISSING_DATA, message, e);
            }
            throw new PrestoException(DELTA_CANNOT_OPEN_SPLIT, message, e);
        }
    }

    public static TupleDomain<ColumnDescriptor> getParquetTupleDomain(Map<List<String>, RichColumnDescriptor> descriptorsByPath, TupleDomain<DeltaColumnHandle> effectivePredicate)
    {
        if (effectivePredicate.isNone()) {
            return TupleDomain.none();
        }

        ImmutableMap.Builder<ColumnDescriptor, Domain> predicate = ImmutableMap.builder();
        for (Map.Entry<DeltaColumnHandle, Domain> entry : effectivePredicate.getDomains().get().entrySet()) {
            DeltaColumnHandle columnHandle = entry.getKey();

            RichColumnDescriptor descriptor = descriptorsByPath.get(ImmutableList.of(columnHandle.getName()));
            if (descriptor != null) {
                predicate.put(descriptor, entry.getValue());
            }
        }
        return TupleDomain.withColumnDomains(predicate.build());
    }

    public static Optional<org.apache.parquet.schema.Type> getParquetType(
            MessageType messageType,
            DeltaColumnHandle column)
    {
        org.apache.parquet.schema.Type type = getParquetTypeByName(column.getName(), messageType);
        return Optional.of(type);
    }
}
