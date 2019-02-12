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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.spi.pipeline.PushdownExpression;
import com.facebook.presto.spi.pipeline.PushdownFunction;
import com.facebook.presto.spi.pipeline.PushdownInExpression;
import com.facebook.presto.spi.pipeline.PushdownInputColumn;
import com.facebook.presto.spi.pipeline.PushdownLiteral;
import com.facebook.presto.spi.pipeline.PushdownLogicalBinaryExpression;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SymbolReference;

import java.util.ArrayList;
import java.util.List;

class PushdownExpressionGenerator
        extends AstVisitor<PushdownExpression, Void>
{
    @Override
    protected PushdownExpression visitFunctionCall(FunctionCall node, Void context)
    {
        List<Expression> inputs = node.getArguments();
        List<PushdownExpression> pushdownInputs = new ArrayList<>();

        for (Expression input : inputs) {
            PushdownExpression pushdownExpression = this.process(input);
            if (pushdownExpression == null) {
                return null;
            }
            pushdownInputs.add(pushdownExpression);
        }

        return new PushdownFunction(node.getName().toString(), pushdownInputs);
    }

    @Override
    protected PushdownExpression visitComparisonExpression(ComparisonExpression node, Void context)
    {
        PushdownExpression left = this.process(node.getLeft());
        PushdownExpression right = this.process(node.getRight());
        String operator = node.getOperator().getValue();

        if (left == null || right == null) {
            return null;
        }

        return new PushdownLogicalBinaryExpression(left, operator, right);
    }

    @Override
    protected PushdownExpression visitBetweenPredicate(BetweenPredicate node, Void context)
    {
        return null;
    }

    @Override
    protected PushdownExpression visitInPredicate(InPredicate node, Void context)
    {
        List<PushdownExpression> arguments = new ArrayList<>();
        if (!(node.getValueList() instanceof InListExpression)) {
            return null;
        }

        InListExpression inList = (InListExpression) node.getValueList();
        for (Expression inValue : inList.getValues()) {
            PushdownExpression out = this.process(inValue);
            if (out == null) {
                return null;
            }

            arguments.add(out);
        }

        PushdownExpression value = this.process(node.getValue());
        if (value == null) {
            return null;
        }

        return new PushdownInExpression(value, arguments);
    }

    @Override
    protected PushdownExpression visitDoubleLiteral(DoubleLiteral node, Void context)
    {
        return new PushdownLiteral(null, null, node.getValue());
    }

    @Override
    protected PushdownExpression visitLongLiteral(LongLiteral node, Void context)
    {
        return new PushdownLiteral(null, node.getValue(), null);
    }

    @Override
    protected PushdownExpression visitStringLiteral(StringLiteral node, Void context)
    {
        return new PushdownLiteral(node.getValue(), null, null);
    }

    @Override
    protected PushdownExpression visitSymbolReference(SymbolReference node, Void context)
    {
        return new PushdownInputColumn(node.getName());
    }

    @Override
    protected PushdownExpression visitCast(Cast node, Void context)
    {
        // Handle cast where the input is already in required type
        Expression input = node.getExpression();
        String type = node.getType();

        if (input instanceof StringLiteral && type.equalsIgnoreCase("varchar")) {
            return this.process(input);
        }

        if (input instanceof LongLiteral && type.equalsIgnoreCase("long")) {
            return this.process(input);
        }

        if (input instanceof DoubleLiteral && type.equalsIgnoreCase("double")) {
            return this.process(input);
        }

        return null;
    }

    @Override
    protected PushdownExpression visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context)
    {
        PushdownExpression left = this.process(node.getLeft());
        PushdownExpression right = this.process(node.getRight());
        String operator = node.getOperator().toString();

        if (left == null || right == null) {
            return null;
        }

        return new PushdownLogicalBinaryExpression(left, operator, right);
    }
}
