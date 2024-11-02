/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.utils;

import com.google.common.collect.ImmutableList;
import org.apache.spark.sql.catalyst.expressions.*;
import org.jetbrains.annotations.NotNull;
import org.opensearch.sql.ast.expression.*;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.ast.tree.Trendline;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.ppl.CatalystPlanContext;
import org.opensearch.sql.ppl.CatalystQueryPlanVisitor;
import scala.Option;
import scala.Tuple2;

import java.util.List;
import java.util.stream.Collectors;

import static org.opensearch.sql.ppl.utils.DataTypeTransformer.seq;

public interface TrendlineCatalystUtils {

    static List<NamedExpression> visitTrendlineComputations(CatalystQueryPlanVisitor.ExpressionAnalyzer expressionAnalyzer, List<Trendline.TrendlineComputation> computations, CatalystPlanContext context) {
        return computations.stream()
                .map(computation -> visitTrendlineComputation(expressionAnalyzer, computation, context))
                .collect(Collectors.toList());
    }

    static NamedExpression visitTrendlineComputation(CatalystQueryPlanVisitor.ExpressionAnalyzer expressionAnalyzer, Trendline.TrendlineComputation node, CatalystPlanContext context) {
        //window lower boundary
        expressionAnalyzer.visitLiteral(new Literal(Math.negateExact(node.getNumberOfDataPoints() - 1), DataType.INTEGER), context);
        Expression windowLowerBoundary = context.popNamedParseExpressions().get();

        //window definition
        // windowspecdefinition(specifiedwindowframe(RowFrame, -2, currentrow$()
        WindowSpecDefinition windowDefinition = new WindowSpecDefinition(
                seq(),
                seq(),
                new SpecifiedWindowFrame(RowFrame$.MODULE$, windowLowerBoundary, CurrentRow$.MODULE$));

        if (node.getComputationType() == Trendline.TrendlineType.SMA) {
            //calculate avg value of the data field
            expressionAnalyzer.visitAggregateFunction(new AggregateFunction(BuiltinFunctionName.AVG.name(), node.getDataField()), context);
            Expression avgFunction = context.popNamedParseExpressions().get();

            //sma window
            WindowExpression sma = new WindowExpression(
                    avgFunction,
                    windowDefinition);

            CaseWhen smaOrNull = trendlineOrNullWhenThereAreTooFewDataPoints(expressionAnalyzer, sma, node, context);

            return org.apache.spark.sql.catalyst.expressions.Alias$.MODULE$.apply(smaOrNull,
                            node.getAlias(),
                            NamedExpression.newExprId(),
                            seq(new java.util.ArrayList<String>()),
                            Option.empty(),
                            seq(new java.util.ArrayList<String>()));
        } else if (node.getComputationType() == Trendline.TrendlineType.WMA) {

            //WMA Logic
            System.out.println("Test");
//            return null;
            return getWMAComputationExpression(expressionAnalyzer, node, context);

        } else {
            throw new IllegalArgumentException(node.getComputationType()+" is not supported");
        }
    }

    /**
     * Produce a Spark Logical Plan in the form NamedExpression with given WindowSpecDefinition.
     *
     */
    private static NamedExpression getWMAComputationExpression(CatalystQueryPlanVisitor.ExpressionAnalyzer expressionAnalyzer, Trendline.TrendlineComputation node, CatalystPlanContext context) {

        System.out.println("Func from WMA!");
        //window lower boundary
        expressionAnalyzer.visitLiteral(new Literal(Math.negateExact(node.getNumberOfDataPoints() - 1), DataType.INTEGER), context);
        Expression windowLowerBoundary = context.popNamedParseExpressions().get();

        // The field name
        expressionAnalyzer.visitLiteral(new Literal(node.getDataField(), DataType.STRING), context);
        Expression dataField = context.popNamedParseExpressions().get();

        //window definition
        WindowSpecDefinition windowDefinition = new WindowSpecDefinition(
                seq(),
                seq(SortUtils.sortOrder(dataField, true)),
                new SpecifiedWindowFrame(RowFrame$.MODULE$, windowLowerBoundary, CurrentRow$.MODULE$));

        // Divider
        expressionAnalyzer.visitLiteral(new Literal((node.getNumberOfDataPoints() * (node.getNumberOfDataPoints()+1) / 2),
                DataType.INTEGER), context);
        Expression divider = context.popNamedParseExpressions().get();

        // Aggregation
        Expression WMAExpression = addInBulk(
                getWindowExpression(expressionAnalyzer, node, context, windowDefinition, 1),
                getWindowExpression(expressionAnalyzer, node, context, windowDefinition, 2),
                getWindowExpression(expressionAnalyzer, node, context, windowDefinition, 3));

        return getAlias(node.getAlias(), new Divide(WMAExpression, divider));
    }

    private static @NotNull Expression getWindowExpression(CatalystQueryPlanVisitor.ExpressionAnalyzer expressionAnalyzer,
                                                                 Trendline.TrendlineComputation node,
                                                                 CatalystPlanContext context,
                                                                 WindowSpecDefinition windowDefinition, int offSet) {

        expressionAnalyzer.visitLiteral(new Literal(offSet, DataType.INTEGER), context);
        Expression offSetInLiteral =  context.popNamedParseExpressions().get();
        Expression exp = getNthExpression(expressionAnalyzer, context, node.getDataField(), offSet);
        //sma window
        return new Multiply(new WindowExpression(exp, windowDefinition), offSetInLiteral);
    }

    private static @NotNull Expression getNthExpression(
            CatalystQueryPlanVisitor.ExpressionAnalyzer expressionAnalyzer,
            CatalystPlanContext context,
            UnresolvedExpression dataField, int offSet) {
        Function func =  new Function(BuiltinFunctionName.NTH_VALUE.name(),
                List.of(dataField, new Literal(offSet, DataType.INTEGER)));

        expressionAnalyzer.visitFunction(func, context);
        return context.popNamedParseExpressions().get();
    }

    private static Expression addInBulk(Expression ... expressions) {
        if (expressions.length > 1) {
            Expression sum = expressions[0];
            for (int i = 1; i < expressions.length; i++) {
                sum = new Add (sum, expressions[i]);
            }
            return sum;
        } else {
            return null;
        }
    }


    private static CaseWhen trendlineOrNullWhenThereAreTooFewDataPoints(CatalystQueryPlanVisitor.ExpressionAnalyzer expressionAnalyzer, WindowExpression trendlineWindow, Trendline.TrendlineComputation node, CatalystPlanContext context) {
        //required number of data points
        expressionAnalyzer.visitLiteral(new Literal(node.getNumberOfDataPoints(), DataType.INTEGER), context);
        Expression requiredNumberOfDataPoints = context.popNamedParseExpressions().get();

        //count data points function
        expressionAnalyzer.visitAggregateFunction(new AggregateFunction(BuiltinFunctionName.COUNT.name(), new Literal(1, DataType.INTEGER)), context);
        Expression countDataPointsFunction = context.popNamedParseExpressions().get();
        //count data points window
        WindowExpression countDataPointsWindow = new WindowExpression(
                countDataPointsFunction,
                trendlineWindow.windowSpec());

        expressionAnalyzer.visitLiteral(new Literal(null, DataType.NULL), context);
        Expression nullLiteral = context.popNamedParseExpressions().get();
        Tuple2<Expression, Expression> nullWhenNumberOfDataPointsLessThenRequired = new Tuple2<>(
                new LessThan(countDataPointsWindow, requiredNumberOfDataPoints),
                nullLiteral
        );
        return new CaseWhen(seq(nullWhenNumberOfDataPointsLessThenRequired), Option.apply(trendlineWindow));
    }


    private static NamedExpression getAlias(String name, Expression expression) {
        return org.apache.spark.sql.catalyst.expressions.Alias$.MODULE$.apply(expression,
                name,
                NamedExpression.newExprId(),
                seq(new java.util.ArrayList<String>()),
                Option.empty(),
                seq(new java.util.ArrayList<String>()));
    }
}
