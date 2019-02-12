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
package com.facebook.presto.spi.pipeline;

import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class TablePipelineNode
        extends PipelineNode
{
    private final String schemaName;
    private final String tableName;
    private final List<String> inputColumns;
    private final List<String> outputColumns;
    private final List<Type> rowType;

    @JsonCreator
    public TablePipelineNode(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("inputColumns") List<String> inputColumns,
            @JsonProperty("outputColumns") List<String> outputColumns,
            @JsonProperty("rowType") List<Type> rowType)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.inputColumns = requireNonNull(inputColumns, "inputColumns is null");
        this.outputColumns = requireNonNull(outputColumns, "outputColumns is null");
        this.rowType = requireNonNull(rowType, "rowType is null");
    }

    @Override
    public PipelineType getType()
    {
        return PipelineType.TABLE;
    }

    @JsonProperty
    public List<String> getInputColumns()
    {
        return inputColumns;
    }

    @JsonProperty
    @Override
    public List<String> getOutputColumns()
    {
        return outputColumns;
    }

    @JsonProperty
    @Override
    public List<Type> getRowType()
    {
        return rowType;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @Override
    public <R, C> R accept(TableScanPipelineVisitor<R, C> visitor, C context)
    {
        return visitor.visitTableNode(this, context);
    }

    @Override
    public String toString()
    {
        return tableName + "(" + outputColumns.stream().collect(Collectors.joining(",")) + ")";
    }
}
