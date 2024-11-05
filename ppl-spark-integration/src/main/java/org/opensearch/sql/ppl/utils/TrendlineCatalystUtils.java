/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.utils;

import org.apache.spark.sql.catalyst.expressions.*;
import org.jetbrains.annotations.NotNull;
import org.opensearch.sql.ast.expression.*;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.tree.Trendline;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.ppl.CatalystPlanContext;
import org.opensearch.sql.ppl.CatalystQueryPlanVisitor;
import scala.Option;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.opensearch.sql.ppl.utils.DataTypeTransformer.seq;

public interface TrendlineCatalystUtils {


    static List<NamedExpression> visitTrendlineComputations(CatalystQueryPlanVisitor.ExpressionAnalyzer expressionAnalyzer, List<Trendline.TrendlineComputation> computations, Optional<Field> sortField, CatalystPlanContext context) {
        return computations.stream()
                .map(computation -> visitTrendlineComputation(expressionAnalyzer, computation, sortField, context))
                .collect(Collectors.toList());
    }


    static NamedExpression visitTrendlineComputation(CatalystQueryPlanVisitor.ExpressionAnalyzer expressionAnalyzer, Trendline.TrendlineComputation node, Optional<Field> sortField, CatalystPlanContext context) {

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
            if (sortField.isPresent()) {
                return getWMAComputationExpression(expressionAnalyzer, node, sortField.get(), context);
            } else {
                throw new IllegalArgumentException(node.getComputationType()+" require a sort field for computation");
            }
        } else {
            throw new IllegalArgumentException(node.getComputationType()+" is not supported");
        }
    }

    /**
     * Produce a Spark Logical Plan in the form NamedExpression with given WindowSpecDefinition.
     *
     */
    private static NamedExpression getWMAComputationExpression(CatalystQueryPlanVisitor.ExpressionAnalyzer analyzer,
                                                               Trendline.TrendlineComputation node,
                                                               Field sortField,
                                                               CatalystPlanContext context) {

        System.out.println("Func 2");
        //window lower boundary
        Expression windowLowerBoundary = getIntExpression(analyzer, context,
                Math.negateExact(node.getNumberOfDataPoints() - 1));
        // The field name
        Expression dataField = getStringExpression(analyzer, context, node.getDataField());
        //window definition
        WindowSpecDefinition windowDefinition = getCommonWindowDefinition(
                analyzer.analyze(sortField, context),
                SortUtils.isSortedAscending(sortField),
                windowLowerBoundary);
        // Divider
        Expression divider = getIntExpression(analyzer, context,
                (node.getNumberOfDataPoints() * (node.getNumberOfDataPoints()+1) / 2));
        // Aggregation
        Expression WMAExpression = sum(
                getNthValueAggregations(analyzer, node, context, windowDefinition, node.getNumberOfDataPoints()));

        return getAlias(node.getAlias(), new Divide(WMAExpression, divider));
    }

    static Expression getIntExpression(CatalystQueryPlanVisitor.ExpressionAnalyzer expressionAnalyzer, CatalystPlanContext context,  int i) {
        expressionAnalyzer.visitLiteral(new Literal(i,
                DataType.INTEGER), context);
        return context.popNamedParseExpressions().get();

    }

    static Expression getStringExpression(CatalystQueryPlanVisitor.ExpressionAnalyzer expressionAnalyzer, CatalystPlanContext context,  UnresolvedExpression exp) {
        expressionAnalyzer.visitLiteral(new Literal(exp,
                DataType.STRING), context);
        return context.popNamedParseExpressions().get();

    }

    static WindowSpecDefinition getCommonWindowDefinition(Expression dataField, boolean ascending, Expression windowLowerBoundary) {
        return new WindowSpecDefinition(
                seq(),
                seq(SortUtils.sortOrder(dataField, ascending)),
                new SpecifiedWindowFrame(RowFrame$.MODULE$, windowLowerBoundary, CurrentRow$.MODULE$));
    }

    private static @NotNull List<Expression> getNthValueAggregations(CatalystQueryPlanVisitor.ExpressionAnalyzer expressionAnalyzer,
                                                                     Trendline.TrendlineComputation node,
                                                                     CatalystPlanContext context,
                                                                     WindowSpecDefinition windowDefinition, int offSet) {

        List<Expression> expressions = new ArrayList<Expression>();
        for (int i = 1; i <= offSet; i++) {
            // Get the offset parameter
            Literal offSetLiteral = new Literal(i, DataType.INTEGER);
            expressionAnalyzer.visitLiteral(offSetLiteral, context);
            Expression offSetExpression =  context.popNamedParseExpressions().get();

            // Composite the nth_value expression.
            Function func =  new Function(BuiltinFunctionName.NTH_VALUE.name(),
                    List.of(node.getDataField(), offSetLiteral));

            expressionAnalyzer.visitFunction(func, context);
            Expression nthValueExp = context.popNamedParseExpressions().get();
            expressions.add(
                    new Multiply(new WindowExpression(nthValueExp, windowDefinition), offSetExpression));
        }
        return expressions;
    }


//    private static Expression sum(List<Expression>  expressions) {
//        if (expressions.size() > 1) {
//            Expression sum = expressions.get(0);
//            for (int i = 1; i < expressions.size(); i++) {
//                sum = new Add (sum, expressions.get(i));
//            }
//            return sum;
//        } else {
//            return null;
//        }
//    }

    private static Expression sum(List<Expression> expressions) {
        return expressions.stream()
                .reduce(Add::new)
                .orElse(null);
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
