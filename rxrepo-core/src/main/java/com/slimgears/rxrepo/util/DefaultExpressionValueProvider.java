package com.slimgears.rxrepo.util;

import com.slimgears.rxrepo.expressions.PropertyExpression;

import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class DefaultExpressionValueProvider<S, T, V> implements PropertyExpressionValueProvider<S, T, V> {
    private final static ConcurrentHashMap<PropertyExpression<?, ?, ?>, PropertyExpressionValueProvider<?, ?, ?>> cache = new ConcurrentHashMap<>();

    @SuppressWarnings("unchecked")
    public static <S, T, V> PropertyExpressionValueProvider<S, T, V> fromProperty(PropertyExpression<S, T, V> propertyExpression) {
        return (PropertyExpressionValueProvider<S, T, V>)cache.computeIfAbsent(propertyExpression, DefaultExpressionValueProvider::new);
    }

    private final Function<S, V> extractor;
    private final PropertyExpression<S, T, V> propertyExpression;

    private DefaultExpressionValueProvider(PropertyExpression<S, T, V> propertyExpression) {
        this.extractor = Expressions.compile(propertyExpression);
        this.propertyExpression = propertyExpression;
    }


    @Override
    public PropertyExpression<S, T, V> property() {
        return propertyExpression;
    }

    @Override
    public V value(S object) {
        return extractor.apply(object);
    }
}
