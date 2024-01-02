package com.slimgears.rxrepo.sql;

import com.google.common.collect.Streams;
import com.slimgears.rxrepo.expressions.ConstantExpression;
import com.slimgears.rxrepo.expressions.Expression;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.util.ExpressionTextGenerator;
import com.slimgears.util.stream.Lazy;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DefaultSqlExpressionGenerator implements SqlExpressionGenerator {
    private final Lazy<ExpressionTextGenerator> sqlGenerator;

    public DefaultSqlExpressionGenerator() {
        this.sqlGenerator = Lazy.of(this::createBuilder)
                .map(ExpressionTextGenerator.Builder::build);
    }

    protected ExpressionTextGenerator.Builder createBuilder() {
        return ExpressionTextGenerator.builder()
                .add(Expression.Type.IsNull, "(%s is null)")
                .add(Expression.Type.Add, "(%s + %s)")
                .add(Expression.Type.Sub, "(%s - %s)")
                .add(Expression.Type.Mul, "(%s * %s)")
                .add(Expression.Type.Div, "(%s / %s)")
                .add(Expression.Type.Negate, "(-%s)")
                .add(Expression.Type.And, "(%s and %s)")
                .add(Expression.Type.Or, "(%s or %s)")
                .add(Expression.Type.Not, "(not %s)")
                .add(Expression.Type.ValueIn, "(%s in (%s))")
                .add(Expression.Type.Equals, "(%s = %s)")
                .add(Expression.Type.GreaterThan, "(%s > %s)")
                .add(Expression.Type.LessThan, "(%s < %s)")
                .add(Expression.Type.IsEmpty, "(%s = '')")
                .add(Expression.Type.Contains, formatAndFixQuotes("(%s like '%%' + %s + '%%')"))
                .add(Expression.Type.StartsWith, formatAndFixQuotes("(%s like %s + '%%')"))
                .add(Expression.Type.EndsWith, formatAndFixQuotes("(%s like '%%' + %s)"))
                .add(Expression.Type.Matches, "(%s like %s)")
                .add(Expression.Type.Length, "LEN(%s)")
                .add(Expression.Type.Concat, "(%s || %s)")
                .add(Expression.Type.ToLower, "LOWER(%s)")
                .add(Expression.Type.ToUpper, "UPPER(%s)")
                .add(Expression.Type.Trim, "TRIM(%s)")
                .add(Expression.Type.Count, "COUNT(%s)")
                .add(Expression.Type.Average, "AVG(CAST(%s as double))")
                .add(Expression.Type.Min, "MIN(%s)")
                .add(Expression.Type.Max, "MAX(%s)")
                .add(Expression.Type.Sum, "SUM(%s)")
                .add(Expression.Type.StringConstant, "'%s'")
                .add(Expression.Type.AsString, "CONVERT(varchar, %s)")
                .add(Expression.Type.AsBoolean, "%s")
                .add(Expression.Type.AsNumeric, "%s")
                .add(Expression.Type.AsComparable, "%s")
                .add(Expression.Type.SearchText, notSupported())
                .add(Expression.Type.SequenceNumber, this::reduceSequenceNumber)
                .add(Expression.OperationType.Argument, "__argument__")
                .add(Expression.OperationType.Constant, "%s")
                .add(Expression.OperationType.Property, this::reduceProperty)
                .add(Expression.OperationType.Composition, "")
                .add(Expression.ValueType.Null, "null");
    }

    public <T> T withParams(List<Object> params, Callable<T> callable) {
        return sqlGenerator.get().withInterceptor(createInterceptor().combineWith(paramsInterceptor(params)), callable);
    }

    public <S, T> String toSqlExpression(ObjectExpression<S, T> expression) {
        return sqlGenerator.get().generate(expression);
    }

    public <S, T> String toSqlExpression(ObjectExpression<S, T> expression, ObjectExpression<?, S> arg) {
        return sqlGenerator.get().generate(expression, arg);
    }

    public <S, T> String toSqlExpression(ObjectExpression<S, T> expression, String arg) {
        return sqlGenerator.get().generate(expression, arg);
    }

    public <S, T> String toSqlExpression(ObjectExpression<S, T> expression, List<Object> params) {
        return withParams(params, () -> toSqlExpression(expression));
    }

    public <S, T> String toSqlExpression(ObjectExpression<S, T> expression, ObjectExpression<?, S> arg, List<Object> params) {
        return withParams(params, () -> toSqlExpression(expression, arg));
    }

    private static ExpressionTextGenerator.Reducer notSupported() {
        return (exp, str) -> {
            throw new IllegalArgumentException("Not supported expression");
        };
    }

    private <S, T> String reduceSequenceNumber(ObjectExpression<S, T> expression, String[] parts) {
        return reduceProperty(expression,
                Stream.concat(
                        Stream.of(parts),
                        Stream.of(SqlFields.sequenceFieldName))
                .toArray(String[]::new));
    }

    protected String fieldName(String fieldName) {
        return "\"" + fieldName.replace("\"", "") + "\"";
    }

    protected <S, T> String reduceProperty(ObjectExpression<S, T> expression, String[] parts) {
        return fieldName(Streams.concat(
                Arrays.stream(parts).limit(parts.length - 1),
                Stream.of(Optional.ofNullable(parts[parts.length - 1])
                        .filter(p -> !p.isEmpty())
                        .orElse("")))
                .filter(p -> !p.isEmpty())
                .collect(Collectors.joining(".")));
    }

    protected ExpressionTextGenerator.Interceptor createInterceptor() {
        return ExpressionTextGenerator.Interceptor.empty();
    }

    protected static ExpressionTextGenerator.Reducer formatAndFixQuotes(String format) {
        return ExpressionTextGenerator.Reducer.fromFormat(format).andThen(str -> str.replaceAll("' \\+ '", ""));
    }

    private static ExpressionTextGenerator.Interceptor paramsInterceptor(List<Object> params) {
        return ExpressionTextGenerator.Interceptor.ofType(ConstantExpression.class, (visitor, expression, visitSupplier) -> {
            params.add(expression.value());
            return "?";
        });
    }

    protected String reduce(ObjectExpression<?, ?> expression, String... parts) {
        return sqlGenerator.get().reduce(expression, parts);
    }
}
