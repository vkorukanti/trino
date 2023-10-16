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

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.Tuple2;
import io.delta.kernel.utils.Utils;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.type.TypeManager;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class DeltaSplitManager
        implements ConnectorSplitManager
{
    private final String connectorId;
    private final DeltaConfig deltaConfig;
    private final DeltaClient deltaClient;
    private final TypeManager typeManager;

    @Inject
    public DeltaSplitManager(
            DeltaConnectorId connectorId,
            DeltaConfig deltaConfig,
            DeltaClient deltaClient,
            TypeManager typeManager)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.deltaConfig = requireNonNull(deltaConfig, "deltaConfig is null");
        this.deltaClient = requireNonNull(deltaClient, "deltaClient is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        DeltaTableHandle tableHandle = (DeltaTableHandle) table;
        return new DeltaSplitSource(session, tableHandle);
    }

    private class DeltaSplitSource
            implements ConnectorSplitSource
    {
        private final DeltaTable deltaTable;
        private final Row scanState;
        private final CloseableIterator<ColumnarBatch> scanFileBatchIterator;
        private final int maxBatchSize;

        // working state
        private CloseableIterator<Row> scanFileIterator;

        DeltaSplitSource(ConnectorSession session, DeltaTableHandle deltaTableHandle)
        {
            this.deltaTable = deltaTableHandle.getDeltaTable();
            Tuple2<Row, CloseableIterator<ColumnarBatch>> stateAndSplits =
                    deltaClient.getScanStateAndSplits(session, deltaTable);
            this.scanState = stateAndSplits._1;
            this.scanFileBatchIterator = stateAndSplits._2;
            this.maxBatchSize = deltaConfig.getMaxSplitsBatchSize();
        }

        @Override
        public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
        {
            List<ConnectorSplit> splits = new ArrayList<>();

            while (splits.size() < maxSize && splits.size() < maxBatchSize) {
                Optional<Row> scanFile = getNextScanFile();
                if (scanFile.isEmpty()) {
                    break;
                }
                splits.add(
                        new DeltaSplit(
                                connectorId,
                                deltaTable.getSchemaName(),
                                deltaTable.getTableName(),
                                deltaTable.getTableLocation(),
                                DeltaRowWrapper.convertRowToJson(scanState),
                                DeltaRowWrapper.convertRowToJson(scanFile.get())
                        )
                );
            }

            return completedFuture(new ConnectorSplitBatch(splits, !scanFileIterator.hasNext()));
        }

        @Override
        public void close()
        {
            Utils.closeCloseables(scanFileIterator, scanFileBatchIterator);
        }

        @Override
        public boolean isFinished()
        {
            return !scanFileIterator.hasNext();
        }

        private Optional<Row> getNextScanFile()
        {
            if (scanFileIterator == null || !scanFileIterator.hasNext()) {
                Utils.closeCloseables(scanFileIterator);

                if (scanFileBatchIterator.hasNext()) {
                    scanFileIterator = scanFileBatchIterator.next().getRows();
                }
                else {
                    return Optional.empty();
                }
            }

            if (scanFileIterator.hasNext()) {
                return Optional.of(scanFileIterator.next());
            }
            else {
                return Optional.empty();
            }
        }
    }
}
