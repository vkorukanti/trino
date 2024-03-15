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
package io.trino.plugin.deltalake.kernel.clients;

import io.delta.kernel.client.JsonHandler;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;

import java.io.IOException;
import java.util.Optional;

public class KernelJsonHandler
        implements JsonHandler
{
    @Override
    public ColumnarBatch parseJson(ColumnVector columnVector, StructType structType, Optional<ColumnVector> optional)
    {
        return null;
    }

    @Override
    public StructType deserializeStructType(String s)
    {
        return null;
    }

    @Override
    public CloseableIterator<ColumnarBatch> readJsonFiles(CloseableIterator<FileStatus> closeableIterator, StructType structType, Optional<Predicate> optional)
            throws IOException
    {
        return null;
    }
}
