package com.slimgears.rxrepo.orientdb;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.reflect.TypeToken;
import com.slimgears.rxrepo.expressions.*;
import com.slimgears.rxrepo.expressions.internal.BooleanBinaryOperationExpression;
import com.slimgears.rxrepo.sql.DefaultSqlExpressionGenerator;
import com.slimgears.rxrepo.util.ExpressionTextGenerator;
import com.slimgears.rxrepo.util.PropertyExpressions;
import com.slimgears.rxrepo.util.PropertyMetas;
import com.slimgears.rxrepo.util.SearchTextUtils;
import com.slimgears.util.autovalue.annotations.HasMetaClass;
import com.slimgears.util.generic.MoreStrings;

import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class OrientDbSqlExpressionGenerator extends DefaultSqlExpressionGenerator {
    private final ExpressionTextGenerator.Interceptor searchTextInterceptor = ExpressionTextGenerator.Interceptor.builder()
            .intercept(Expression.OperationType.Binary, ExpressionTextGenerator.Interceptor.ofType(BooleanBinaryOperationExpression.class, this::onVisitBinaryExpression))
            .intercept(Expression.Type.SearchText, ExpressionTextGenerator.Interceptor.ofType(BooleanBinaryOperationExpression.class, this::onVisitSearchTextExpression))
            .build();

    @Override
    protected ExpressionTextGenerator.Builder createBuilder() {
        return super.createBuilder()
                .add(Expression.Type.AsString, "%s.asString()")
                .add(Expression.Type.Average, "AVG(%s.convert('double'))");
    }

    @Override
    protected ExpressionTextGenerator.Interceptor createInterceptor() {
        return searchTextInterceptor;
    }

    private String onVisitSearchTextExpression(Function<? super ObjectExpression<?, ?>, String> visitor, BooleanBinaryOperationExpression<?, ?, String> expression, Supplier<String> visitedExpression) {
        String concat = PropertyExpressions.searchableProperties(expression.left())
                .map(PropertyExpression::asString)
                .map(visitor)
                .collect(Collectors.joining(" + ' ' + "));

        String searchExpression = super.toSqlExpression(expression.right());
        return formatAndFixQuotes("((%s) containsText %s)").reduce(expression, concat, searchExpression);
    }

    private String onVisitBinaryExpression(Function<? super ObjectExpression<?, ?>, String> visitor, BooleanBinaryOperationExpression<?, ?, ?> expression, Supplier<String> visitedExpression) {
        if (!requiresAsStringMapping(expression)) {
            return visitedExpression.get();
        }

        String left = visitBinaryArgument(visitor, expression.left());
        String right = visitBinaryArgument(visitor, expression.right());

        return super.reduce(expression, left, right);
    }

    private boolean requiresAsStringMapping(BooleanBinaryOperationExpression<?, ?, ?> expression) {
        return (expression.left().type().operationType() == Expression.OperationType.Property ||
                expression.right().type().operationType() == Expression.OperationType.Property) &&
                PropertyMetas.isEmbedded(expression.left().reflect().objectType());
    }

    private String visitBinaryArgument(Function<? super ObjectExpression<?, ?>, String> visitor, ObjectExpression<?, ?> expression) {
        if (expression.type().operationType() == Expression.OperationType.Property) {
            return (visitor.apply(expression) + "`AsString`").replace("``", "");
        } else if (expression.type().operationType() == Expression.OperationType.Constant) {
            ConstantExpression<?, ?> constantExpression = (ConstantExpression<?, ?>)expression;
            Object value = constantExpression.value();
            TypeToken<?> valueType = constantExpression.objectType();
            if (PropertyMetas.isEmbedded(valueType)) {
                return visitor.apply(ConstantExpression.of(String.valueOf(value)));
            } else if (valueType.isSubtypeOf(Collection.class)) {
                value = ((Collection<?>)value)
                        .stream()
                        .map(val -> val instanceof HasMetaClass ? val.toString() : val)
                        .collect(ImmutableList.toImmutableList());
                return visitor.apply(ConstantExpression.of(value));
            }
        }

        return visitor.apply(expression);
    }

    private String searchTextToWildcard(String searchText) {
        searchText = searchText.replaceAll("([:+\\-*(){}\\[\\]\\\\/;%])", "?");
        searchText = Arrays
                .stream(searchText.split("\\s"))
                .map(t -> "+*" + t + "*")
                .collect(Collectors.joining(" && "));
        return searchText;
    }
}
