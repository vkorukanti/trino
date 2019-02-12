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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class PushdownInExpression
        extends PushdownExpression
{
    private final PushdownExpression value;
    private final List<PushdownExpression> arguments;

    @JsonCreator
    public PushdownInExpression(
            @JsonProperty("value") PushdownExpression value,
            @JsonProperty("arguments") List<PushdownExpression> arguments)
    {
        this.value = requireNonNull(value, "value is null");
        this.arguments = requireNonNull(arguments, "arguments is null");
    }

    @JsonProperty
    public PushdownExpression getValue()
    {
        return value;
    }

    @JsonProperty
    public List<PushdownExpression> getArguments()
    {
        return arguments;
    }

    @Override
    public String toString()
    {
        return value + " IN " + "(" + arguments.stream().map(a -> a.toString()).collect(Collectors.joining(", ")) + ")";
    }

    @Override
    public <R, C> R accept(PushdownExpresssionVisitor<R, C> visitor, C context)
    {
        return visitor.visitInExpression(this, context);
    }
}
