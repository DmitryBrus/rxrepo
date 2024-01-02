package com.slimgears.rxrepo.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.slimgears.rxrepo.expressions.Expression;
import com.slimgears.rxrepo.expressions.ExpressionVisitor;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.expressions.UnaryOperationExpression;
import com.slimgears.util.autovalue.annotations.PropertyMeta;
import com.slimgears.util.generic.ScopedInstance;
import com.slimgears.util.stream.Optionals;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.slimgears.util.stream.Optionals.ofType;

@SuppressWarnings("WeakerAccess")
public class ExpressionTextGenerator {
    private final ScopedInstance<Interceptor> scopedInterceptor = ScopedInstance.create(Interceptor.empty());

    private static <T> T[] requireArgs(T[] args, int count) {
        if (args.length < count) {
            throw new IllegalArgumentException("Expected " + count + " arguments, actual: " + args.length);
        }
        return args;
    }

    public interface GenericInterceptor<E extends ObjectExpression> {
        String onVisit(Function<? super ObjectExpression<?, ?>, String> visitor, E expression, Supplier<String> visitSupplier);
    }

    public interface Interceptor {
        String onVisit(Function<? super ObjectExpression<?, ?>, String> visitor, ObjectExpression<?, ?> exp, Supplier<String> visitedResult);

        default Interceptor combineWith(Interceptor other) {
            return (visitor, exp, visitedResult) -> other.onVisit(visitor, exp, () -> this.onVisit(visitor, exp, visitedResult));
        }

        static Interceptor empty() {
            return (visitor, exp, visitedResult) -> visitedResult.get();
        }

        static <E extends ObjectExpression> Interceptor ofType(Class<E> expressionType, GenericInterceptor<E> interceptor) {
            return (visitor, exp, visitedResult) -> Optional
                    .of(exp)
                    .flatMap(Optionals.ofType(expressionType))
                    .map(e -> interceptor.onVisit(visitor, e, visitedResult))
                    .orElseGet(visitedResult);
        }

        static InterceptorBuilder builder() {
            return new InterceptorBuilder();
        }
    }

    public static class InterceptorBuilder {
        private final ImmutableMap.Builder<Expression.Type, Interceptor> interceptorsByType = ImmutableMap.builder();
        private final ImmutableMap.Builder<Expression.OperationType, Interceptor> interceptorsByOpType = ImmutableMap.builder();
        private final ImmutableMap.Builder<Expression.ValueType, Interceptor> interceptorsByValueType = ImmutableMap.builder();

        public InterceptorBuilder intercept(Expression.Type type, Interceptor interceptor) {
            interceptorsByType.put(type, interceptor);
            return this;
        }

        public InterceptorBuilder intercept(Expression.OperationType type, Interceptor interceptor) {
            interceptorsByOpType.put(type, interceptor);
            return this;
        }

        public InterceptorBuilder intercept(Expression.ValueType type, Interceptor interceptor) {
            interceptorsByValueType.put(type, interceptor);
            return this;
        }

        public Interceptor build() {
            Map<Expression.Type, Interceptor> byType = interceptorsByType.build();
            Map<Expression.OperationType, Interceptor> byOpType = interceptorsByOpType.build();
            Map<Expression.ValueType, Interceptor> byOpValueType = interceptorsByValueType.build();

            return (visitor, exp, visitedResult) -> Optionals.or(
                    () -> Optional.ofNullable(byType.get(exp.type())),
                    () -> Optional.ofNullable(byOpType.get(exp.type().operationType())),
                    () -> Optional.ofNullable(byOpValueType.get(exp.type().valueType())))
                    .orElseGet(Interceptor::empty)
                    .onVisit(visitor, exp, visitedResult);
        }
    }

    public interface Reducer {
        String reduce(ObjectExpression<?, ?> expression, String... parts);

        static Reducer fromFormat(String format) {
            return (exp, args) -> String.format(format, (Object[])args);
        }

        static Reducer fromBinary(BiFunction<String, String, String> reducer) {
            return (exp, args) -> reducer.apply(requireArgs(args, 2)[0], args[1]);
        }

        static Reducer fromUnary(Function<String, String> reducer) {
            return (exp, args) -> reducer.apply(requireArgs(args, 1)[0]);
        }

        static Reducer just(String str) {
            return (exp, args) -> str;
        }

        static Reducer join(String delimiter) {
            return (exp, args) -> Arrays
                    .stream(args)
                    .filter(a -> !a.isEmpty())
                    .collect(Collectors.joining(delimiter));
        }

        default Reducer andThen(Function<String, String> postProcess) {
            return (exp, args) -> postProcess.apply(this.reduce(exp, args));
        }
    }

    private final ImmutableMap<Expression.Type, Reducer> typeToReducer;
    private final ImmutableMap<Expression.OperationType, Reducer> opTypeToReducer;
    private final ImmutableMap<Expression.ValueType, Reducer> valueTypeToReducer;

    public static Builder builder() {
        return new Builder();
    }

    private ExpressionTextGenerator(ImmutableMap<Expression.Type, Reducer> typeToReducer, ImmutableMap<Expression.OperationType, Reducer> opTypeToReducer, ImmutableMap<Expression.ValueType, Reducer> valueTypeToReducer) {
        this.typeToReducer = typeToReducer;
        this.opTypeToReducer = opTypeToReducer;
        this.valueTypeToReducer = valueTypeToReducer;
    }

    public static class Builder {
        private final Map<Expression.Type, Reducer> typeToReducerBuilder = Maps.newHashMap();
        private final Map<Expression.OperationType, Reducer> opTypeToReducerBuilder = Maps.newHashMap();
        private final Map<Expression.ValueType, Reducer> valTypeToReducerBuilder = Maps.newHashMap();

        public Builder add(Expression.Type type, String format) {
            return add(type, Reducer.fromFormat(format));
        }

        public Builder add(Expression.Type type, Reducer reducer) {
            typeToReducerBuilder.put(type, reducer);
            return this;
        }

        public Builder add(Expression.OperationType type, Reducer reducer) {
            opTypeToReducerBuilder.put(type, reducer);
            return this;
        }

        public Builder add(Expression.OperationType type, String format) {
            return add(type, Reducer.fromFormat(format));
        }

        public Builder add(Expression.ValueType type, String format) {
            return add(type, Reducer.fromFormat(format));
        }

        public Builder add(Expression.ValueType type, Reducer reducer) {
            valTypeToReducerBuilder.put(type, reducer);
            return this;
        }

        public ExpressionTextGenerator build() {
            return new ExpressionTextGenerator(
                    ImmutableMap.copyOf(typeToReducerBuilder),
                    ImmutableMap.copyOf(opTypeToReducerBuilder),
                    ImmutableMap.copyOf(valTypeToReducerBuilder));
        }
    }

    public <T> T withInterceptor(Interceptor interceptor, Callable<T> action) {
        return scopedInterceptor.withScope(interceptor, action);
    }

    public <S, T> String generate(ObjectExpression<S, T> expression) {
        return generate(expression, "");
    }
    
    public <S, T> String generate(ObjectExpression<S, T> expression, ObjectExpression<?, S> arg) {
        String argStr = generate(arg);
        return generate(expression, argStr);
    }
    
    public String generate(ObjectExpression<?, ?> expression, String arg) {
        Visitor visitor = createVisitor();
        return visitor.visit(expression, arg);
    }

    public String reduce(ObjectExpression<?, ?> expression, String... parts) {
        return toReducer(expression.type()).reduce(expression, parts);
    }

    protected Visitor createVisitor() {
        return new Visitor();
    }

    private Reducer toReducer(Expression.Type type) {
        return Optionals.or(
                () -> reducerFromExpressionType(type),
                () -> reducerFromOperationType(type.operationType()),
                () -> reducerFromValueType(type.valueType()))
                .orElseGet(() -> Reducer.just("%s"));
    }

    private Optional<Reducer> reducerFromExpressionType(Expression.Type type) {
        return Optional.ofNullable(typeToReducer.get(type));
    }

    private Optional<Reducer> reducerFromValueType(Expression.ValueType type) {
        return Optional.ofNullable(valueTypeToReducer.get(type));
    }

    private Optional<Reducer> reducerFromOperationType(Expression.OperationType type) {
        return Optional.ofNullable(opTypeToReducer.get(type));
    }

    protected class Visitor extends ExpressionVisitor<String, String> {
        @Override
        public String visit(Expression expression, String arg) {
            Supplier<String> visited = () -> super.visit(expression, arg);
            return Optional.of(expression)
                    .flatMap(ofType(ObjectExpression.class))
                    .map(objExp -> scopedInterceptor.current().onVisit(exp -> visit(exp, arg), objExp, visited))
                    .orElseGet(visited);
        }

        @Override
        protected String reduceBinary(ObjectExpression<?, ?> expression, Expression.Type type, String first, String second) {
            return toReducer(type).reduce(expression, first, second);
        }

        @Override
        protected String reduceUnary(ObjectExpression<?, ?> expression, Expression.Type type, String first) {
            return toReducer(type).reduce(expression, first);
        }

        @Override
        protected <S, T> String visitOther(ObjectExpression<S, T> expression, String context) {
            return toReducer(expression.type()).reduce(expression);
        }

        @Override
        protected <S, T, R> String visitUnaryOperator(UnaryOperationExpression<S, T, R> expression, String context) {
            return super.visitUnaryOperator(expression, context);
        }

        @Override
        protected <T, V> String visitProperty(PropertyMeta<T, V> propertyMeta, String context) {
            return propertyMeta.name();
        }

        @Override
        protected <T> String visitArgument(TypeToken<T> argType, String context) {
            return context;
        }

        @Override
        protected <V> String visitConstant(Expression.Type type, V value, String context) {
            if (value == null) {
                return reduceUnary(null, Expression.Type.NullConstant, null);
            } else if (value instanceof String) {
                return reduceUnary(null, Expression.Type.StringConstant, (String)value);
            } else if (value instanceof Number) {
                return reduceUnary(null, Expression.Type.NumericConstant, String.valueOf(value));
            } else {
                return reduceUnary(null, type, String.valueOf(value));
            }
        }
    }
}
