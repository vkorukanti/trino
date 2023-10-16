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
package io.trino.delta.client;

import io.delta.kernel.client.DefaultFileHandler;
import io.delta.kernel.client.FileReadContext;
import io.delta.kernel.client.ParquetHandler;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FileDataReadResult;
import io.delta.kernel.data.Row;
import io.delta.kernel.fs.FileStatus;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.Utils;
import io.trino.delta.data.TrinoDeltaColumnarBatch;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.ReaderPageSource;
import io.trino.plugin.hive.parquet.ParquetPageSourceFactory;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.predicate.TupleDomain;
import org.apache.hadoop.conf.Configuration;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class TrinoDeltaParquetHandler
        extends DefaultFileHandler
        implements ParquetHandler
{
    private final Configuration configuration;
    private final TrinoFileSystem fileSystem;

    public TrinoDeltaParquetHandler(Configuration configuration, TrinoFileSystem fileSystem)
    {
        this.configuration = configuration;
        this.fileSystem = fileSystem;
    }

    @Override
    public CloseableIterator<FileDataReadResult> readParquetFiles(
            CloseableIterator<FileReadContext> fileIter,
            StructType physicalSchema)
            throws IOException
    {
        return fileIter.map(
                fileReadContext -> {

                    FileStatus fileStatus = Utils.getFileStatus(fileReadContext.getScanFileRow());
                    TrinoInputFile inputFile = fileSystem.newInputFile(fileStatus.getPath());


                }
        );
    }

    private CloseableIterator<FileDataReadResult> readFiles(
            CloseableIterator<FileReadContext> fileIter,
            StructType physicalSchema)
    {
        return new CloseableIterator<FileDataReadResult>() {

            private FileReadContext currentFileReadContext = null;
            private CloseableParquetBatchReader currentFileReader = null;

            @Override
            public void close()
                    throws IOException
            {
                Utils.closeCloseables(fileIter, currentFileReader);
            }

            @Override
            public boolean hasNext()
            {
                 if (currentFileReader == null || !currentFileReader.hasNext()) {
                     Utils.closeCloseables(currentFileReader);

                     if (fileIter.hasNext()) {
                         currentFileReadContext = fileIter.next();
                         FileStatus fileStatus = Utils.getFileStatus(currentFileReadContext.getScanFileRow());
                         TrinoInputFile inputFile = fileSystem.newInputFile(fileStatus.getPath());

                         try {
                             ReaderPageSource pageSource = ParquetPageSourceFactory.createPageSource(
                                     inputFile,
                                     0,
                                     inputFile.length(),
                                     createHiveHandles(physicalSchema),
                                     TupleDomain.all(),
                                     true,
                                     DateTimeZone.getDefault(),
                                     new FileFormatDataSourceStats(),
                                     new ParquetReaderOptions(),
                                     Optional.empty(),
                                     100
                             );

                             currentFileReader = new CloseableParquetBatchReader(physicalSchema, pageSource.get());
                         }
                         catch (IOException ioe) {
                             throw new UncheckedIOException(ioe);
                         }
                     } else {
                         return false;
                     }
                 }
                 return currentFileReader.hasNext();
            }

            @Override
            public FileDataReadResult next()
            {
                final ColumnarBatch data = currentFileReader.next();
                final Row scanFileRow = currentFileReadContext.getScanFileRow();
                return new FileDataReadResult() {
                    @Override
                    public ColumnarBatch getData()
                    {
                        return data;
                    }

                    @Override
                    public Row getScanFileRow()
                    {
                        return scanFileRow;
                    }
                };
            }
        };
    }

    private List<HiveColumnHandle> createHiveHandles(StructType deltaSchema)
    {
        List<HiveColumnHandle> hiveColumnHandles = new ArrayList<>();
        for (StructField structField : deltaSchema.fields()) {
            DataType kernelType = structField.getDataType();
            String name = structField.getName();

            if (kernelType instanceof BooleanType) {

            }
        }
    }

    private static class CloseableParquetBatchReader
            implements CloseableIterator<ColumnarBatch>
    {
        private final ConnectorPageSource deltaPageSource;
        private final StructType schema;
        private final Map<String, Integer> columnNameToIndexMap;

        private Page nextPage;

        public CloseableParquetBatchReader(
                StructType schema,
                ConnectorPageSource deltaPageSource)
        {
            this.deltaPageSource = requireNonNull(deltaPageSource, "deltaPageSource is null");
            this.schema = requireNonNull(schema, "schema is null");
            this.columnNameToIndexMap = createColumnNameToIndexMap(schema);
        }

        @Override
        public boolean hasNext()
        {
            if (nextPage != null) {
                return true;
            }
            nextPage = deltaPageSource.getNextPage();

            return nextPage != null;
        }

        @Override
        public ColumnarBatch next()
        {
            Page page = nextPage != null ? nextPage : deltaPageSource.getNextPage();
            nextPage = null;
            return new TrinoDeltaColumnarBatch(
                    schema,
                    page,
                    columnNameToIndexMap);
        }

        @Override
        public void close()
                throws IOException
        {
            deltaPageSource.close();
        }

        private static Map<String, Integer> createColumnNameToIndexMap(StructType schema)
        {
            int index = 0;
            Map<String, Integer> columnNameToIndexMap = new HashMap<>();
            for (StructField field : schema.fields()) {
                columnNameToIndexMap.put(field.getName(), index);
                index++;
            }
            return columnNameToIndexMap;
        }
    }
}
