package com.slimgears.rxrepo.mongodb;

import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import com.slimgears.rxrepo.expressions.Expression;
import com.slimgears.rxrepo.expressions.ExpressionVisitor;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.mongodb.adapter.MongoFieldMapper;
import com.slimgears.rxrepo.util.Expressions;
import com.slimgears.util.autovalue.annotations.PropertyMeta;
import com.slimgears.util.stream.Optionals;
import org.bson.Document;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Stream;

class MongoExpressionAdapter extends ExpressionVisitor<Void, Object> {
    private final static ImmutableMap<Expression.Type, Reducer> expressionTypeReducers = ImmutableMap
            .<Expression.Type, Reducer>builder()
            .put(Expression.Type.IsNull, args -> expr("$eq", args[0], null))
            .put(Expression.Type.Add, args -> expr("$add", args))
            .put(Expression.Type.Sub, args -> expr("$subtract", args))
            .put(Expression.Type.Mul, args -> expr("$multiply", args))
            .put(Expression.Type.Div, args -> expr("$divide", args))
            .put(Expression.Type.Not, args -> expr("$not", args))
            .put(Expression.Type.And, args -> expr("$and", args))
            .put(Expression.Type.Or, args -> expr("$or", args))
            .put(Expression.Type.Equals, args -> expr("$eq", args))
            .put(Expression.Type.LessThan, args -> expr("$lt", args))
            .put(Expression.Type.GreaterThan, args -> expr("$gt", args))
            .put(Expression.Type.ValueIn, args -> expr("$in", args))
            .put(Expression.Type.Length, args -> expr("$strLenCP", args))
            .put(Expression.Type.Concat, args -> expr("$concat", args))
            .put(Expression.Type.AsString, args -> expr("$toString", args))
            .put(Expression.Type.StartsWith, args -> expr("$eq", expr("$indexOfCP", args), 0))
            .put(Expression.Type.SearchText, args -> searchText(args[0], args[1]))
            .put(Expression.Type.EndsWith, args -> expr("$eq",
                    expr("$indexOfCP", args),
                    expr("$subtract",
                            expr("$strLenCP", args[0]),
                            expr("$strLenCP", args[1]))))
            .put(Expression.Type.Contains, args -> expr("$gte", expr("$indexOfCP", args), 0))
            .put(Expression.Type.Count, args -> expr("$sum", expr("$toLong", 1)))
            .put(Expression.Type.Min, args -> expr("$min", "$" + MongoPipeline.valueField))
            .put(Expression.Type.Max, args -> expr("$max", "$" + MongoPipeline.valueField))
            .put(Expression.Type.Sum, args -> expr("$sum", "$" + MongoPipeline.valueField))
            .put(Expression.Type.Average, args -> expr("$avg", "$" + MongoPipeline.valueField))
            //.put(Expression.Type.SequenceNumber, args -> expr(MongoFieldMapper.instance.versionField()))
            .put(Expression.Type.SequenceNumber, args -> expr("$toLong", 0))
            .build();

    private final static ImmutableMap<Expression.OperationType, Reducer> operationTypeReducers = ImmutableMap
            .<Expression.OperationType, Reducer>builder()
            .put(Expression.OperationType.Property, MongoExpressionAdapter::reduceProperties)
            .build();

    private static Document expr(String operator, Object... args) {
        return new Document(operator, args.length == 1 ? args[0] : Arrays.asList(args));
    }

    private static Object reduce(Expression.Type type, Object... args) {
        return Optionals.or(
                () -> Optional.ofNullable(expressionTypeReducers.get(type)),
                () -> Optional.ofNullable(operationTypeReducers.get(type.operationType())))
                .map(r -> r.reduce(args))
                .orElseThrow(() -> new RuntimeException("Cannot reduce expression of type " + type));
    }

    private static Object reduceProperties(Object... parts) {
        String head = parts[0].toString();
        String tail = parts[1].toString();
        return head.equals("$") ? head + tail : head + "." + tail;
    }

    @Override
    protected Object reduceBinary(ObjectExpression<?, ?> expression, Expression.Type type, Object first, Object second) {
        return reduce(type, first, second);
    }

    @Override
    protected Object reduceUnary(ObjectExpression<?, ?> expression, Expression.Type type, Object first) {
        return reduce(type, first);
    }

    @Override
    protected <T, V> Object visitProperty(PropertyMeta<T, V> propertyMeta, Void arg) {
        return MongoFieldMapper.instance.toFieldName(propertyMeta);
    }

    @Override
    protected <V> Object visitConstant(Expression.Type type, V value, Void arg) {
        return (value instanceof String && ((String)value).contains("$"))
                ? new Document().append("$literal", value)
                : value;
    }

    @Override
    protected <T> Object visitArgument(TypeToken<T> argType, Void arg) {
        return "$";
    }

    private static Document searchText(Object target, Object searchExpr) {
        String[] parts = searchExpr.toString().split("\\s");
        return expr("$and", Stream.of(parts)
                .map(p -> reduce(Expression.Type.Contains, target + MongoFieldMapper.instance.searchableTextField(), p))
                .toArray(Object[]::new));
    }

    interface Reducer {
        Object reduce(Object... args);
    }
}
