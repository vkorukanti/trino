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

import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;
import io.prestosql.plugin.base.classloader.ClassLoaderSafeConnectorMetadata;
import io.prestosql.plugin.base.classloader.ClassLoaderSafeConnectorPageSourceProvider;
import io.prestosql.plugin.base.classloader.ClassLoaderSafeConnectorSplitManager;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.session.PropertyMetadata;
import io.prestosql.spi.transaction.IsolationLevel;

import javax.inject.Inject;

import java.util.List;

import static io.prestosql.delta.DeltaTransactionHandle.INSTANCE;
import static java.util.Objects.requireNonNull;

public class DeltaConnector
        implements Connector
{
    private static final Logger log = Logger.get(DeltaConnector.class);

    private final LifeCycleManager lifeCycleManager;
    private final DeltaMetadata metadata;
    private final DeltaSplitManager splitManager;
    private final DeltaSessionProperties sessionProperties;
    private final DeltaPageSourceProvider pageSourceProvider;

    @Inject
    public DeltaConnector(
            LifeCycleManager lifeCycleManager,
            DeltaMetadata metadata,
            DeltaSplitManager splitManager,
            DeltaSessionProperties sessionProperties,
            DeltaPageSourceProvider pageSourceProvider)
    {
        this.lifeCycleManager = requireNonNull(lifeCycleManager, "lifeCycleManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.sessionProperties = requireNonNull(sessionProperties, "sessionProperties is null");
        this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
    }

    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
    {
        return INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
    {
        return new ClassLoaderSafeConnectorMetadata(metadata, getClass().getClassLoader());
    }

    @Override
    public ConnectorSplitManager getSplitManager()
    {
        return new ClassLoaderSafeConnectorSplitManager(splitManager, getClass().getClassLoader());
    }

    @Override
    public ConnectorPageSourceProvider getPageSourceProvider()
    {
        return new ClassLoaderSafeConnectorPageSourceProvider(pageSourceProvider, getClass().getClassLoader());
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties.getSessionProperties();
    }

    @Override
    public final void shutdown()
    {
        try {
            lifeCycleManager.stop();
        }
        catch (Exception e) {
            log.error(e, "Error shutting down connector");
        }
    }
}
