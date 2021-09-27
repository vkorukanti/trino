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

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonModule;
import io.prestosql.plugin.base.CatalogName;
import io.prestosql.plugin.hive.HiveHdfsModule;
import io.prestosql.plugin.hive.NodeVersion;
import io.prestosql.plugin.hive.authentication.HiveAuthenticationModule;
import io.prestosql.plugin.hive.azure.HiveAzureModule;
import io.prestosql.plugin.hive.gcs.HiveGcsModule;
import io.prestosql.plugin.hive.metastore.HiveMetastoreModule;
import io.prestosql.plugin.hive.s3.HiveS3Module;
import io.prestosql.spi.NodeManager;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.connector.ConnectorHandleResolver;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

public class DeltaConnectorFactory
        implements ConnectorFactory
{
    @Override
    public String getName()
    {
        return "delta";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new DeltaConnectionHandleResolver();
    }

    @Override
    public Connector create(String catalogName, Map<String, String> requiredConfig, ConnectorContext context)
    {
        requireNonNull(requiredConfig, "requiredConfig is null");
        try {
            Bootstrap app = new Bootstrap(
                    new JsonModule(),
                    new DeltaModule(catalogName, context.getTypeManager()),
                    new HiveS3Module(),
                    new HiveAzureModule(),
                    new HiveGcsModule(),
                    new HiveHdfsModule(),
                    new HiveAuthenticationModule(),
                    new HiveMetastoreModule(Optional.empty()),
                    binder -> {
                        binder.bind(NodeVersion.class).toInstance(new NodeVersion(context.getNodeManager().getCurrentNode().getVersion()));
                        binder.bind(NodeManager.class).toInstance(context.getNodeManager());
                        binder.bind(CatalogName.class).toInstance(new CatalogName(catalogName));
                    });

            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(requiredConfig)
                    .initialize();

            return injector.getInstance(DeltaConnector.class);
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }
}
