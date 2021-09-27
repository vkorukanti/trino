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
import io.delta.kernel.client.TableClient;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.DataReadResult;
import io.delta.kernel.data.Row;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.Utils;
import io.trino.delta.data.AbstractTrinoDeltaVector;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static io.trino.delta.DeltaErrorCode.DELTA_READ_DATA_ERROR;
import static java.util.Objects.requireNonNull;

public class DeltaScanRowPageSource
        implements ConnectorPageSource
{
    private final TableClient tableClient;
    private final Row scanState;
    private final Row scanFile;
    private final List<DeltaColumnHandle> columns;

    private CloseableIterator<DataReadResult> batchIter;
    private boolean isFinished;

    public DeltaScanRowPageSource(
            TableClient tableClient,
            Row scanState,
            Row scanFile,
            List<DeltaColumnHandle> columns)
    {
        this.tableClient = tableClient;
        this.scanState = scanState;
        this.scanFile = scanFile;
        this.columns = columns;
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return isFinished;
    }

    @Override
    public Page getNextPage()
    {
        try {
            if (batchIter == null) {
                batchIter = Scan.readData(
                        tableClient,
                        scanState,
                        Utils.singletonCloseableIterator(scanFile),
                        Optional.empty());
            }
            if (!batchIter.hasNext()) {
                isFinished = true;
                return null;
            }
            DataReadResult nextBatch = batchIter.next();
            Page page = convertDeltaToTrino(nextBatch.getData(), columns);

            if (nextBatch.getSelectionVector().isPresent()) {
                return withSelectionPositions(page, nextBatch.getSelectionVector().get());
            }
            return page;
        }
        catch (TrinoException e) {
            closeWithSuppression(e);
            throw e; // already properly handled exception - throw without any additional info
        }
        catch (RuntimeException | IOException e) {
            closeWithSuppression(e);
            throw new TrinoException(DELTA_READ_DATA_ERROR, e);
        }
    }

    @Override
    public long getMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
            throws IOException
    {
        batchIter.close();
    }

    private void closeWithSuppression(Throwable throwable)
    {
        requireNonNull(throwable, "throwable is null");
        try {
            close();
        }
        catch (Exception e) {
            // Self-suppression not permitted
            if (e != throwable) {
                throwable.addSuppressed(e);
            }
        }
    }

    private static Page convertDeltaToTrino(
            ColumnarBatch columnarBatch,
            List<DeltaColumnHandle> columns)
    {
        List<Block> blocks = new ArrayList<>();
        int i = 0;
        for (DeltaColumnHandle column : columns) {
            ColumnVector deltaVector = columnarBatch.getColumnVector(i);
            if (deltaVector instanceof AbstractTrinoDeltaVector trinoDeltaVector) {
                blocks.add(trinoDeltaVector.getTrinoBlock());
            }
            else {
                throw new UnsupportedOperationException("Encountered vectors that are not Trino based.");
            }
            i++;
        }
        return new Page(columnarBatch.getSize(), blocks.toArray(new Block[0]));
    }

    private static Page withSelectionPositions(
            Page page,
            ColumnVector selectionColumnVector)
    {
        int positionCount = page.getPositionCount();
        int[] retained = new int[positionCount];
        int retainedCount = 0;
        for (int position = 0; position < positionCount; position++) {
            if (selectionColumnVector.getBoolean(position)) {
                retained[retainedCount] = position;
                retainedCount++;
            }
        }
        if (retainedCount == positionCount) {
            return page;
        }
        return page.getPositions(retained, 0, retainedCount);
    }
}
