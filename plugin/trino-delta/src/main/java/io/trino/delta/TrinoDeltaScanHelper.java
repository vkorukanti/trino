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

import io.delta.standalone.core.DeltaScanHelper;
import io.delta.standalone.core.RowIndexFilter;
import io.delta.standalone.data.ColumnarRowBatch;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;
import io.delta.standalone.utils.CloseableIterator;
import io.trino.plugin.hive.ReaderPageSource;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSource;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class TrinoDeltaScanHelper
        implements DeltaScanHelper, Serializable
{
    @Override
    public CloseableIterator<ColumnarRowBatch> readParquetFile(
            String filePath,
            StructType readSchema,
            TimeZone timeZone,
            RowIndexFilter deletionVector,
            Object opaqueObj)
    {
        Function<String, ReaderPageSource> pageSourceCreator =
                (Function<String, ReaderPageSource>) opaqueObj;
        ReaderPageSource readerPageSource = pageSourceCreator.apply(filePath);
        return new CloseableParquetBatchReader(readSchema, readerPageSource.get());
    }

    @Override
    public DataInputStream readDeletionVectorFile(String filePath)
    {
//        Path path = new Path(filePath);
//        try {
//            HdfsContext hdfsContext =
//            FileSystem fs = hdfsEnvironment.getFileSystem(hdfsContext, path);
//            return fs.open(path);
//        } catch (IOException e) {
//            Throwables.throwIfUnchecked(e);
//        }
        return null;
    }

    public static class CloseableParquetBatchReader
            implements CloseableIterator<ColumnarRowBatch>
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
        public ColumnarRowBatch next()
        {
            Page page = nextPage != null ? nextPage : deltaPageSource.getNextPage();
            nextPage = null;
            return new TrinoDeltaColumnarRowBatch(
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
            for (StructField field : schema.getFields()) {
                columnNameToIndexMap.put(field.getName(), index);
                index++;
            }
            return columnNameToIndexMap;
        }
    }
}
