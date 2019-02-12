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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "@type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = PushdownLiteral.class, name = "literal"),
        @JsonSubTypes.Type(value = PushdownInputColumn.class, name = "inputColumn"),
        @JsonSubTypes.Type(value = PushdownLogicalBinaryExpression.class, name = "comparison"),
        @JsonSubTypes.Type(value = PushdownFunction.class, name = "function"),
        @JsonSubTypes.Type(value = PushdownInExpression.class, name = "in")})
public abstract class PushdownExpression
{
    public <R, C> R accept(PushdownExpresssionVisitor<R, C> visitor, C context)
    {
        return visitor.visitExpression(this, context);
    }
}
