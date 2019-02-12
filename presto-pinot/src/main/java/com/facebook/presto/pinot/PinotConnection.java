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
package com.facebook.presto.pinot;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import com.linkedin.pinot.common.data.Schema;
import io.airlift.log.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class PinotConnection
{
    private static final Logger log = Logger.get(PinotConnection.class);
    private final PinotClusterInfoFetcher pinotClusterInfoFetcher;
    private final LoadingCache<String, List<PinotColumn>> pinotTableColumnCache =
            CacheBuilder.newBuilder()
                    .expireAfterWrite(1, TimeUnit.DAYS)
                    .build(new CacheLoader<String, List<PinotColumn>>()
                    {
                        @Override
                        public List<PinotColumn> load(String tableName)
                                throws Exception
                        {
                            Schema tablePinotSchema = pinotClusterInfoFetcher.getTableSchema(tableName);
                            return PinotColumnUtils.getPinotColumnsForPinotSchema(tablePinotSchema);
                        }
                    });

    private final LoadingCache<String, PinotTable> pinotTableCache =
            CacheBuilder.newBuilder()
                    .expireAfterWrite(1, TimeUnit.DAYS)
                    .build(new CacheLoader<String, PinotTable>()
                    {
                        @Override
                        public PinotTable load(String tableName)
                                throws Exception
                        {
                            List<PinotColumn> columns = getPinotColumnsForTable(tableName);
                            return new PinotTable(tableName, columns);
                        }
                    });

    private final LoadingCache<String, PinotColumn> pinotTimeColumnPerTableCache =
            CacheBuilder.newBuilder()
                    .expireAfterWrite(1, TimeUnit.DAYS)
                    .build(new CacheLoader<String, PinotColumn>()
                    {
                        @Override
                        public PinotColumn load(String tableName)
                                throws Exception
                        {
                            Schema pinotTableSchema = pinotClusterInfoFetcher.getTableSchema(tableName);
                            String columnName = pinotTableSchema.getTimeColumnName();
                            return new PinotColumn(columnName, PinotColumnUtils.getPrestoTypeFromPinotType(pinotTableSchema.getFieldSpecFor(columnName)));
                        }
                    });
    private final LoadingCache<String, Map<String, Map<String, List<String>>>> pinotRoutingTableCache =
            CacheBuilder.newBuilder()
                    .expireAfterWrite(1, TimeUnit.DAYS)
                    .build(new CacheLoader<String, Map<String, Map<String, List<String>>>>()
                    {
                        @Override
                        public Map<String, Map<String, List<String>>> load(String tableName)
                                throws Exception
                        {
                            Map<String, Map<String, List<String>>> routingTableForTable = pinotClusterInfoFetcher.getRoutingTableForTable(tableName);
                            log.debug("RoutingTable for table: %s is %s", tableName, Arrays.toString(routingTableForTable.entrySet().toArray()));
                            return routingTableForTable;
                        }
                    });
    private final LoadingCache<String, Map<String, String>> pinotTimeBoundaryCache =
            CacheBuilder.newBuilder()
                    .expireAfterWrite(1, TimeUnit.DAYS)
                    .build(new CacheLoader<String, Map<String, String>>()
                    {
                        @Override
                        public Map<String, String> load(String tableName)
                                throws Exception
                        {
                            Map<String, String> timeBoundaryForTable = pinotClusterInfoFetcher.getTimeBoundaryForTable(tableName);
                            log.debug("TimeBoundary for table: %s is %s", tableName, Arrays.toString(timeBoundaryForTable.entrySet().toArray()));
                            return timeBoundaryForTable;
                        }
                    });
    private AtomicReference<List<String>> allTablesCache = new AtomicReference<>();

    @Inject
    public PinotConnection(PinotClusterInfoFetcher pinotClusterInfoFetcher)
    {
        this.pinotClusterInfoFetcher = pinotClusterInfoFetcher;
    }

    public Map<String, Map<String, List<String>>> GetRoutingTable(String table)
            throws Exception
    {
        return pinotRoutingTableCache.get(table);
    }

    public Map<String, String> GetTimeBoundary(String table)
            throws Exception
    {
        return pinotTimeBoundaryCache.get(table);
    }

    public List<String> getTableNames()
            throws Exception
    {
        List<String> allTables = allTablesCache.get();
        if (allTables == null) {
            synchronized (allTablesCache) {
                allTables = allTablesCache.get();
                if (allTables == null) {
                    allTables = pinotClusterInfoFetcher.getAllTables();
                    allTablesCache.set(allTables);
                }
            }
        }
        return allTables;
    }

    public PinotTable getTable(String tableName)
            throws Exception
    {
        return pinotTableCache.get(tableName);
    }

    private List<PinotColumn> getPinotColumnsForTable(String tableName)
            throws Exception
    {
        return pinotTableColumnCache.get(tableName);
    }

    public PinotColumn getPinotTimeColumnForTable(String tableName)
            throws Exception
    {
        return pinotTimeColumnPerTableCache.get(tableName);
    }
}
