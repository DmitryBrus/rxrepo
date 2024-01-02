package com.slimgears.rxrepo.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import com.slimgears.rxrepo.annotations.Searchable;
import com.slimgears.rxrepo.expressions.*;
import com.slimgears.util.autovalue.annotations.*;
import com.slimgears.util.stream.Optionals;
import com.slimgears.util.stream.Streams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuppressWarnings({"WeakerAccess", "UnstableApiUsage"})
public class PropertyExpressions {
    private final static Logger log = LoggerFactory.getLogger(PropertyExpressions.class);
    private final static Map<PropertyExpression<?, ?, ?>, Collection<PropertyExpression<?, ?, ?>>> relatedMandatoryPropertiesCache = Maps.newConcurrentMap();
    private final static Map<TypeToken<?>, Collection<PropertyExpression<?, ?, ?>>> mandatoryPropertiesCache = Maps.newConcurrentMap();
    private final static Map<PropertyExpression<?, ?, ?>, Collection<PropertyExpression<?, ?, ?>>> parentProperties = Maps.newConcurrentMap();
    private final static Map<MetaClass<?>, Collection<PropertyExpression<?, ?, ?>>> embeddedPropertiesCache = Maps.newConcurrentMap();
    private final static Map<MetaClass<?>, Collection<PropertyExpressionValueProvider<?, ?, ?>>> embeddedPropertiesProviderCache = Maps.newConcurrentMap();
    private final static ImmutableSet<TypeToken<?>> numericTypes = ImmutableSet.<TypeToken<?>>builder()
            .add(TypeToken.of(int.class), TypeToken.of(Integer.class))
            .add(TypeToken.of(short.class), TypeToken.of(Short.class))
            .add(TypeToken.of(long.class), TypeToken.of(Long.class))
            .add(TypeToken.of(byte.class), TypeToken.of(Byte.class))
            .add(TypeToken.of(float.class), TypeToken.of(Float.class))
            .add(TypeToken.of(double.class), TypeToken.of(Double.class))
            .add(TypeToken.of(BigInteger.class), TypeToken.of(BigDecimal.class))
            .build();

    public static String pathOf(PropertyExpression<?, ?, ?> propertyExpression) {
        return Optional.ofNullable(parentOf(propertyExpression))
                .map(PropertyExpression::path)
                .map(p -> p + "." + propertyExpression.property().name())
                .orElseGet(propertyExpression.property()::name);
    }

    public static boolean hasParent(PropertyExpression<?, ?, ?> propertyExpression) {
        return propertyExpression.target() instanceof PropertyExpression;
    }

    public static <K, S> PropertyExpression<S, S, K> keyOf(MetaClassWithKey<K, S> metaClass) {
        return PropertyExpression.ofObject(metaClass.keyProperty());
    }

    public static <S, V> PropertyExpression<S, S, V> fromMeta(PropertyMeta<S, V> meta) {
        return fromMeta(ObjectExpression.arg(meta.declaringType().asType()), meta);
    }

    @SuppressWarnings("unchecked")
    public static <S> Stream<PropertyExpressionValueProvider<S, ?, ?>> valueProvidersFromMeta(MetaClass<S> metaClass) {
        return embeddedPropertiesProviderCache.computeIfAbsent(metaClass, PropertyExpressions::providersOf)
                .stream()
                .map(p -> (PropertyExpressionValueProvider<S, ?, ?>)p);
    }

    @SuppressWarnings("unchecked")
    public static <S> Stream<PropertyExpression<S, ?, ?>> embeddedPropertiesForMeta(MetaClass<S> metaClass) {
        return embeddedPropertiesCache.computeIfAbsent(metaClass, m -> embeddedPropertiesOf(m.asType())
                .map(p -> (PropertyExpression<?, ?, ?>)p)
                .collect(ImmutableList.toImmutableList()))
                .stream()
                .map(p -> (PropertyExpression<S, ?, ?>)p);
    }

    public static <S> boolean isMandatory(PropertyExpression<S, ?, ?> propertyExpression) {
        return propertyExpression != null
                && PropertyMetas.isMandatory(propertyExpression.property())
                && isMandatory(parentOf(propertyExpression));
    }

    private static Collection<PropertyExpressionValueProvider<?, ?, ?>> providersOf(MetaClass<?> metaClass) {
        return embeddedPropertiesOf(metaClass.asType())
                .map(PropertyExpressionValueProvider::fromProperty)
                .collect(ImmutableList.toImmutableList());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static <S, T, V> PropertyExpression<S, T, V> fromMeta(ObjectExpression<S, T> target, PropertyMeta<T, V> propertyMeta) {
        TypeToken<V> type = propertyMeta.type();

        if (type.isSubtypeOf(String.class)) {
            return (PropertyExpression<S, T, V>)PropertyExpression.ofString(target, (PropertyMeta<T, String>)propertyMeta);
        } else if (numericTypes.contains(type) || type.isSubtypeOf(Number.class)) {
            return (PropertyExpression<S, T, V>)PropertyExpression.ofNumeric(target, (PropertyMeta)propertyMeta);
        } else if (type.isSubtypeOf(Boolean.class) || type.equals(TypeToken.of(boolean.class))) {
            return (PropertyExpression<S, T, V>)PropertyExpression.ofBoolean(target, (PropertyMeta)propertyMeta);
        } else if (type.isSubtypeOf(Comparable.class)) {
            return (PropertyExpression<S, T, V>)PropertyExpression.ofComparable(target, (PropertyMeta)propertyMeta);
        } else if (type.isSubtypeOf(Collection.class)) {
            return (PropertyExpression<S, T, V>)PropertyExpression.ofCollection(target, (PropertyMeta)propertyMeta);
        }
        return PropertyExpression.ofObject(target, propertyMeta);
    }

    public static <S> ImmutableSet<PropertyExpression<S, ?, ?>> allReferencedProperties(ObjectExpression<S, ?> expression) {
        if (expression == null) {
            return ImmutableSet.of();
        }

        ImmutableSet.Builder<PropertyExpression<S, ?, ?>> referencedPropertiesBuilder = ImmutableSet.builder();
        ExpressionVisitor<Void, Void> visitor = new ExpressionVisitor<Void, Void>() {
            @Override
            protected Void reduceBinary(ObjectExpression<?, ?> expression, Expression.Type type, Void first, Void second) {
                return null;
            }

            @Override
            protected Void reduceUnary(ObjectExpression<?, ?> expression, Expression.Type type, Void first) {
                return null;
            }

            @SuppressWarnings("unchecked")
            @Override
            protected <_S, T, V> Void visitProperty(PropertyExpression<_S, T, V> expression, Void arg) {
                referencedPropertiesBuilder.add((PropertyExpression<S, ?, ?>)expression);
                return super.visitProperty(expression, arg);
            }

            @Override
            protected <_S, T1, T2, R> Void visitBinaryOperator(BinaryOperationExpression<_S, T1, T2, R> expression, Void arg) {
                if (expression.type() == Expression.Type.SearchText) {
                    PropertyExpressions.searchableProperties(expression.left())
                            .forEach(p -> visitProperty(p, arg));
                    return null;
                } else {
                    return super.visitBinaryOperator(expression, arg);
                }
            }

            @Override
            protected <T, V> Void visitProperty(PropertyMeta<T, V> propertyMeta, Void arg) {
                return null;
            }

            @Override
            protected <V> Void visitConstant(Expression.Type type, V value, Void arg) {
                return null;
            }

            @Override
            protected <T> Void visitArgument(TypeToken<T> argType, Void arg) {
                return null;
            }
        };
        visitor.visit(expression, null);
        return referencedPropertiesBuilder.build();
    }

    @SuppressWarnings("unchecked")
    public static <T> PropertyExpression<T, ?, ?> parentOf(PropertyExpression<T, ?, ?> property) {
        return (PropertyExpression<T, ?, ?>) Optional.of(property.target())
                .flatMap(Optionals.ofType(PropertyExpression.class))
                .orElse(null);
    }

    @SuppressWarnings("unchecked")
    public static <T> PropertyExpression<T, T, ?> rootOf(PropertyExpression<T, ?, ?> property) {
        return hasParent(property) ? rootOf(parentOf(property)) : (PropertyExpression<T, T, ?>)property;
    }

    public static <T> Stream<PropertyExpression<T, ?, ?>> propertiesOf(TypeToken<T> type) {
        return PropertyMetas.hasMetaClass(type)
                ? propertiesOf(ObjectExpression.arg(type), new HashSet<>())
                : Stream.empty();
    }

    public static <T> Stream<PropertyExpression<T, ?, ?>> embeddedPropertiesOf(TypeToken<T> type) {
        return PropertyMetas.hasMetaClass(type)
                ? embeddedPropertiesOf(ObjectExpression.arg(type), new HashSet<>())
                : Stream.empty();
    }

    public static <T> Stream<PropertyExpression<T, T, ?>> ownPropertiesOf(TypeToken<T> type) {
        return PropertyMetas.hasMetaClass(type)
                ? ownPropertiesOf(ObjectExpression.arg(type))
                : Stream.empty();
    }

    public static <S, T> Stream<PropertyExpression<S, T, ?>> ownPropertiesOf(ObjectExpression<S, T> target) {
        MetaClass<T> metaClass = MetaClasses.forTokenUnchecked(target.reflect().objectType());
        return Streams
                .fromIterable(metaClass.properties())
                .map(prop -> PropertyExpression.ofObject(target, prop));
    }

    private static <S, T> Stream<PropertyExpression<S, ?, ?>> embeddedPropertiesOf(ObjectExpression<S, T> target, Set<PropertyMeta<?, ?>> visitedProps) {
        List<PropertyExpression<S, T, ?>> ownProps = ownPropertiesOf(target)
                .filter(p -> visitedProps.add(p.property()))
                .collect(Collectors.toList());

        return Stream.concat(
                ownProps.stream(),
                ownProps.stream()
                        .filter(p -> PropertyMetas.isEmbedded(p.property()))
                        .flatMap(p -> embeddedPropertiesOf(p, visitedProps)));
    }

    private static <S, T> Stream<PropertyExpression<S, ?, ?>> propertiesOf(ObjectExpression<S, T> target, Set<PropertyMeta<?, ?>> visitedProps) {
        List<PropertyExpression<S, T, ?>> ownProps = ownPropertiesOf(target)
                .filter(p -> visitedProps.add(p.property()))
                .collect(Collectors.toList());

        return Stream.concat(
                ownProps.stream(),
                ownProps.stream()
                        .filter(p -> PropertyMetas.hasMetaClass(p.reflect().objectType()))
                        .flatMap(p -> propertiesOf(p, visitedProps)));
    }

    public static <T, V> PropertyExpression<T, ?, V> fromPath(TypeToken<T> origin, String path) {
        return createExpressionFromPath(ObjectExpression.arg(origin), path);
    }

    public static <T, V> PropertyExpression<T, ?, V> fromPath(Class<T> origin, String path) {
        return fromPath(TypeToken.of(origin), path);
    }

    @SuppressWarnings("unchecked")
    public static <T, V> Function<T, V> toGetter(PropertyExpression<T, ?, V> propertyExpression) {
        if (propertyExpression.target() instanceof PropertyExpression) {
            Function<T, ?> getter = toGetter((PropertyExpression<T, ?, ?>)propertyExpression.target());
            return getter.andThen(val -> Optional
                    .ofNullable(val)
                    .map(v -> ((PropertyMeta<Object, V>)propertyExpression.property()).getValue(v))
                    .orElse(null));
        }
        return ((PropertyMeta<T, V>)propertyExpression.property())::getValue;
    }

    private static <S, T, V> PropertyExpression<S, ?, V> createExpressionFromPath(ObjectExpression<S, T> target, String path) {
        String head = head(path);
        MetaClass<T> meta = Objects.requireNonNull(MetaClasses.forTokenUnchecked(target.reflect().objectType()));
        PropertyMeta<T, V> prop = meta.getProperty(head);
        if (head.length() == path.length()) {
            return PropertyExpression.ofObject(target, prop);
        }
        return createExpressionFromPath(PropertyExpression.ofObject(target, prop), path.substring(head.length() + 1));
    }

    private static String head(String path) {
        int pos = path.indexOf('.');
        return pos >= 0 ? path.substring(0, pos) : path;
    }

    public static boolean propertyEquals(PropertyExpression<?, ?, ?> property, Object other) {
        return Optional.ofNullable(other)
                .filter(p -> property.hashCode() == p.hashCode())
                .flatMap(Optionals.ofType(PropertyExpression.class))
                .map(p -> Objects.equals(property.target(), p.target()) &&
                        Objects.equals(property.property(), p.property()))
                .orElse(false);
    }

    public static <T> ImmutableSet<PropertyExpression<T, ?, ?>> includeMandatoryProperties(TypeToken<T> typeToken, ImmutableSet<PropertyExpression<T, ?, ?>> properties) {
        Stream<PropertyExpression<T, ?, ?>> includedProperties = properties.stream()
                .flatMap(PropertyExpressions::mandatoryProperties)
                .distinct();

        if (PropertyMetas.hasMetaClass(typeToken)) {
            includedProperties = Stream.concat(includedProperties, PropertyExpressions.mandatoryProperties(typeToken));
        }

        return includedProperties.collect(ImmutableSet.toImmutableSet());
    }

    @SuppressWarnings("unchecked")
    public static <S> Stream<PropertyExpression<S, ?, ?>> mandatoryProperties(PropertyExpression<S, ?, ?> exp) {
        return relatedMandatoryPropertiesCache.computeIfAbsent(
                exp,
                PropertyExpressions::mandatoryPropertiesNotCached)
                .stream()
                .map(p -> (PropertyExpression<S, ?, ?>)p);
    }

    @SuppressWarnings("unchecked")
    public static <S> Stream<PropertyExpression<S, ?, ?>> mandatoryProperties(TypeToken<S> typeToken) {
        return mandatoryPropertiesCache.computeIfAbsent(
                typeToken,
                PropertyExpressions::mandatoryPropertiesNotCached)
                .stream()
                .map(p -> (PropertyExpression<S, ?, ?>)p);
    }

    private static <S> Collection<PropertyExpression<?, ?, ?>> mandatoryPropertiesNotCached(PropertyExpression<S, ?, ?> exp) {
        return Stream.concat(Stream.of(exp), parentProperties(exp)
                .filter(p -> PropertyMetas.hasMetaClass(p.property()))
                .flatMap(p -> mandatoryProperties(p, MetaClasses.forTokenUnchecked(p.reflect().objectType()), new HashSet<>())))
                .collect(Collectors.toCollection(Sets::newLinkedHashSet));
    }

    private static <S> Collection<PropertyExpression<?, ?, ?>> mandatoryPropertiesNotCached(TypeToken<S> typeToken) {
        MetaClass<S> metaClass = MetaClasses.forTokenUnchecked(typeToken);
        return mandatoryProperties(ObjectExpression.arg(metaClass.asType()), metaClass, new HashSet<>())
                .collect(Collectors.toCollection(Sets::newLinkedHashSet));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <S, T> Stream<PropertyExpression<S, ?, ?>> mandatoryProperties(ObjectExpression<S, T> target, MetaClass<T> metaClass, Set<PropertyExpression<S, ?, ?>> visitedProperties) {
        return Streams.fromIterable(metaClass.properties())
                .filter(p -> !p.hasAnnotation(Nullable.class))
                .map(p -> PropertyExpression.ofObject(target, p))
                .filter(visitedProperties::add)
                .flatMap(propertyExpression -> {
                    Stream<PropertyExpression<S, ?, ?>> stream = (Stream<PropertyExpression<S, ?, ?>>) Optional.of(propertyExpression.reflect().objectType())
                            .filter(PropertyMetas::hasMetaClass)
                            .map(MetaClasses::forTokenUnchecked)
                            .map(meta -> mandatoryProperties(propertyExpression, (MetaClass)meta, visitedProperties))
                            .orElseGet(Stream::empty);
                    return Stream.concat(Stream.of(propertyExpression), stream);
                });
    }

    @SuppressWarnings("unchecked")
    public static <S, T, V> Stream<PropertyExpression<S, ?, ?>> parentProperties(PropertyExpression<S, T, V> property) {
        return ((Collection<PropertyExpression<S, ?, ?>>)(Collection<?>)parentProperties.computeIfAbsent(property, p -> parentPropertiesNonCached(property).collect(Collectors.toList())))
                .stream();
    }

    @SuppressWarnings("unchecked")
    public static <S, T> ImmutableSet<PropertyExpression<S, ?, ?>> unmapProperties(ImmutableSet<PropertyExpression<T, ?, ?>> properties, ObjectExpression<S, T> mapping) {
        return mapping != null
                ? Optional.ofNullable(properties)
                .<ImmutableSet<PropertyExpression<S, ?, ?>>>map(pp -> pp.stream().map(p -> unmapProperty(p, mapping)).collect(ImmutableSet.toImmutableSet()))
                .orElse(null)
                : (ImmutableSet<PropertyExpression<S, ?, ?>>)(ImmutableSet<?>)properties;
    }

    public static boolean isReference(PropertyExpression<?, ?, ?> propertyExpression) {
        return PropertyMetas.isReference(propertyExpression.property());
    }

    private static boolean isSearchable(PropertyMeta<?, ?> property) {
        return property.hasAnnotation(Searchable.class);
    }

    public static <S, T> Stream<PropertyExpression<S, ?, ?>> searchableProperties(ObjectExpression<S, T> parent) {
        TypeToken<T> objectType = parent.reflect().objectType();
        MetaClass<T> metaClass = MetaClasses.forTokenUnchecked(objectType);
        return Streams.fromIterable(metaClass.properties())
                .filter(PropertyExpressions::isSearchable)
                .map(p -> fromMeta(parent, p))
                .flatMap(p -> PropertyMetas.isReference(p.property())
                        ? searchableProperties(p)
                        : Stream.of(p));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static <S, T, V> PropertyExpression<T, ?, V> tailOf(PropertyExpression<S, ?, V> property, ObjectExpression<S, T> head) {
        if (Objects.equals(property.target(), head)) {
            PropertyMeta<T, V> propertyMeta = (PropertyMeta<T, V>)property.property();
            return PropertyExpression.ofObject(ObjectExpression.arg(propertyMeta.declaringType().asType()), propertyMeta);
        }
        if (property.target() instanceof PropertyExpression) {
            return PropertyExpression.ofObject(tailOf((PropertyExpression)property.target(), head), property.property());
        }
        return (PropertyExpression<T, ?, V>)property;
    }

    private static <S, T> PropertyExpression<S, ?, ?> unmapProperty(PropertyExpression<T, ?, ?> propertyExpression, ObjectExpression<S, T> mapping) {
        PropertyExpression<S, ?, ?> unmappedProp = Expressions.compose(mapping, propertyExpression);
        log.trace("Unmapped property (mapping: {}) {} -> {}", mapping, propertyExpression, unmappedProp);
        return unmappedProp;
    }

    private static <S, T, V> Stream<PropertyExpression<S, ?, ?>> parentPropertiesNonCached(PropertyExpression<S, T, V> property) {
        return Optional
                .ofNullable(parentOf(property))
                .map(t -> Stream.concat(Stream.of(t), parentPropertiesNonCached(t)))
                .orElseGet(Stream::empty);
    }
}
