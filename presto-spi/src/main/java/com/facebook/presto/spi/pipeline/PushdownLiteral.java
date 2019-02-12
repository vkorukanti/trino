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

public class PushdownLiteral
        extends PushdownExpression
{
    private final String stringValue;
    private final Long longValue;
    private final Double doubleValue;

    @JsonCreator
    public PushdownLiteral(
            @JsonProperty("stringValue") String stringValue,
            @JsonProperty("longValue") Long longValue,
            @JsonProperty("doubleValue") Double doubleValue)
    {
        // TODO: check only one of them is non-null
        this.stringValue = stringValue;
        this.longValue = longValue;
        this.doubleValue = doubleValue;
    }

    @JsonProperty
    public String getStringValue()
    {
        return stringValue;
    }

    @JsonProperty
    public Long getLongValue()
    {
        return longValue;
    }

    @JsonProperty
    public Double getDoubleValue()
    {
        return doubleValue;
    }

    @Override
    public String toString()
    {
        if (stringValue != null) {
            return "'" + stringValue + "'";
        }

        if (longValue != null) {
            return String.valueOf(longValue);
        }

        if (doubleValue != null) {
            return String.valueOf(doubleValue);
        }

        return null;
    }

    @Override
    public <R, C> R accept(PushdownExpresssionVisitor<R, C> visitor, C context)
    {
        return visitor.visitLiteral(this, context);
    }
}
