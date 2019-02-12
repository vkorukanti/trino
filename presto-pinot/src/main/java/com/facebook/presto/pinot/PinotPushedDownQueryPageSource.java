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

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.pipeline.AggregationPipelineNode;
import com.facebook.presto.spi.pipeline.PipelineNode;
import com.facebook.presto.spi.pipeline.TableScanPipeline;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Locale.ENGLISH;

public class PinotPushedDownQueryPageSource
        implements ConnectorPageSource
{
    private static final CloseableHttpClient httpClient = HttpClients.createDefault();
    private static final String REQUEST_PAYLOAD_TEMPLATE = "{\"pql\" : \"%s\" }";
    private static final String QUERY_URL_TEMPLATE = "http://%s/query";

    private final TableScanPipeline scanPipeline;
    private final PinotConfig pinotConfig;
    private final List<PinotColumnHandle> columnHandles;

    private boolean finished;
    private long readTimeNanos;

    public PinotPushedDownQueryPageSource(PinotConfig pinotConfig, TableScanPipeline scanPipeline, List<PinotColumnHandle> columnHandles)
    {
        this.pinotConfig = pinotConfig;
        this.scanPipeline = scanPipeline;
        this.columnHandles = columnHandles;
    }

    private static void setValue(Type type, BlockBuilder blockBuilder, String value)
    {
        if (type instanceof BigintType) {
            blockBuilder.writeLong(Double.valueOf(value).longValue());
        }
        else if (type instanceof IntegerType) {
            blockBuilder.writeInt(Double.valueOf(value).intValue());
        }
        else if (type instanceof VarcharType) {
            Slice slice = Slices.utf8Slice(value);
            blockBuilder.writeBytes(slice, 0, slice.length()).closeEntry();
        }
        else if (type instanceof DoubleType) {
            type.writeDouble(blockBuilder, Double.valueOf(value));
        }
        else {
            throw new UnsupportedOperationException("type '" + type + "' not supported");
        }
    }

    private static String getColumnName(AggregationPipelineNode.Node node)
    {
        if (node.getNodeType() == AggregationPipelineNode.NodeType.AGGREGATE) {
            AggregationPipelineNode.Aggregation aggregation = (AggregationPipelineNode.Aggregation) node;

            // TODO: handle multi input functions
            return aggregation.getFunction().toLowerCase(ENGLISH) + "_" +
                    (aggregation.getInputs().isEmpty() ? "star" : aggregation.getInputs().get(0).toLowerCase(ENGLISH));
        }

        AggregationPipelineNode.GroupByColumn groupByColumn = (AggregationPipelineNode.GroupByColumn) node;

        return groupByColumn.getInputColumn().toLowerCase(ENGLISH);
    }

    private static AggregationPipelineNode getAggPipelineNode(TableScanPipeline pipeline)
    {
        for (PipelineNode node : pipeline.getPipelineNodes()) {
            if (node instanceof AggregationPipelineNode) {
                return (AggregationPipelineNode) node;
            }
        }

        return null;
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public Page getNextPage()
    {
        if (finished) {
            return null;
        }

        String psql = PinotPushdownQueryGenerator.generate(scanPipeline);

        long start = System.nanoTime();
        try {
            HttpUriRequest request = RequestBuilder.post(String.format(QUERY_URL_TEMPLATE, pinotConfig.getProxyRestUrl()))
                    .setHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                    .setHeader("RPC-Caller", "presto")
                    .setHeader("RPC-Service", pinotConfig.getProxyRestService())
                    .setEntity(new StringEntity(String.format(REQUEST_PAYLOAD_TEMPLATE, psql)))
                    .build();

            String response = httpClient.execute(request,
                    httpResponse -> {
                        int status = httpResponse.getStatusLine().getStatusCode();
                        if (status >= 200 && status < 300) {
                            HttpEntity entity = httpResponse.getEntity();
                            return entity != null ? EntityUtils.toString(entity) : null;
                        }
                        throw new ClientProtocolException("Unexpected Pinot response status: " + status);
                    });

            JSONObject jsonBody = JSONObject.parseObject(response);

            JSONArray aggResults = jsonBody.getJSONArray("aggregationResults");

            ImmutableList.Builder<Type> types = ImmutableList.builder();
            columnHandles
                    .stream()
                    .map(columnHandle -> columnHandle.getColumnType())
                    .forEach(types::add);
            List<Type> columnTypes = types.build();

            PageBuilder pageBuilder = new PageBuilder(columnTypes);
            AggregationPipelineNode aggPipelineNode = getAggPipelineNode(scanPipeline);

            Map<String, BlockBuilder> columnBlockBuilderMap = new HashMap<>();
            Map<String, Type> columnTypeMap = new HashMap<>();
            for (int columnIndex = 0; columnIndex < aggPipelineNode.getNodes().size(); columnIndex++) {
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(columnIndex);
                String columnName = getColumnName(aggPipelineNode.getNodes().get(columnIndex));
                columnBlockBuilderMap.put(columnName, blockBuilder);
                columnTypeMap.put(columnName, aggPipelineNode.getNodes().get(columnIndex).getOutputType());
            }

            int counter = 0;
            for (int aggrIndex = 0; aggrIndex < aggResults.size(); aggrIndex++) {
                JSONObject result = aggResults.getJSONObject(aggrIndex);
                JSONArray groupByColumns = result.getJSONArray("groupByColumns");
                String function = result.getString("function");
                JSONArray groupByResults = result.getJSONArray("groupByResult");

                if (groupByResults != null) {
                    // group by aggregation
                    for (int groupByIndex = 0; groupByIndex < groupByResults.size(); groupByIndex++) {
                        JSONObject row = groupByResults.getJSONObject(groupByIndex);
                        JSONArray group = row.getJSONArray("group");

                        for (int k = 0; k < groupByColumns.size(); k++) {
                            String groupByColumnName = groupByColumns.getString(k).toLowerCase(ENGLISH);

                            BlockBuilder blockBuilder = columnBlockBuilderMap.get(groupByColumnName);
                            Type type = columnTypeMap.get(groupByColumnName);
                            String groupByColumnValue = group.getString(k);

                            setValue(type, blockBuilder, groupByColumnValue);
                        }

                        String value = row.getString("value");
                        BlockBuilder blockBuilder = columnBlockBuilderMap.get(function);
                        Type type = columnTypeMap.get(function);
                        setValue(type, blockBuilder, value);

                        counter++;
                    }
                }
                else {
                    // simple aggregation
                    String value = result.getString("value");

                    BlockBuilder blockBuilder = columnBlockBuilderMap.get(function);
                    Type type = columnTypeMap.get(function);
                    setValue(type, blockBuilder, value);

                    counter++;
                }
            }

            pageBuilder.declarePositions(counter);
            Page page = pageBuilder.build();

            finished = true;
            return page;
        }
        catch (Exception e) {
            Throwables.propagate(e);
        }
        finally {
            readTimeNanos += System.nanoTime() - start;
        }

        return null;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
    {
        finished = true;
    }
}
