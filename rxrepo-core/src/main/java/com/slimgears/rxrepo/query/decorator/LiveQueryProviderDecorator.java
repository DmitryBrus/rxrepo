package com.slimgears.rxrepo.query.decorator;

import com.google.common.collect.ImmutableSet;
import com.slimgears.rxrepo.expressions.Aggregator;
import com.slimgears.rxrepo.expressions.Expression;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.rxrepo.expressions.internal.NumericUnaryOperationExpression;
import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.query.NotificationPrototype;
import com.slimgears.rxrepo.query.Notifications;
import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.rxrepo.query.provider.QueryInfos;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.rxrepo.util.PredicateBuilder;
import com.slimgears.rxrepo.util.PropertyExpressions;
import com.slimgears.rxrepo.util.Queries;
import com.slimgears.util.autovalue.annotations.*;
import com.slimgears.util.stream.Streams;
import io.reactivex.Observable;
import io.reactivex.ObservableTransformer;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class LiveQueryProviderDecorator extends AbstractQueryProviderDecorator {
    private final Duration aggregationDebounceTime;

    private LiveQueryProviderDecorator(QueryProvider upstream, Duration aggregationDebounceTime) {
        super(upstream);
        this.aggregationDebounceTime = aggregationDebounceTime;
    }

    public static QueryProvider.Decorator create(Duration aggregationDebounceTime) {
        return src -> new LiveQueryProviderDecorator(src, aggregationDebounceTime);
    }

    @Override
    public <K, S, T> Observable<Notification<T>> liveQuery(QueryInfo<K, S, T> query) {
        return super.liveQuery(QueryInfo.<K, S, S>builder()
                        .metaClass(query.metaClass())
                        .properties(QueryInfos.allReferencedProperties(query))
                        .build())
                .compose(applyReferencedObserve(query))
                .compose(Notifications.applyFilter(query.predicate()))
                .compose(Notifications.applyMap(query.mapping()))
                .compose(Notifications.applyFieldsFilter(query.properties()));
    }

    @Override
    public <K, S, T> Observable<Notification<T>> queryAndObserve(QueryInfo<K, S, T> queryInfo, QueryInfo<K, S, T> observeInfo) {
        return Queries.queryAndObserve(
                query(queryInfo),
                liveQuery(observeInfo));
    }

    @Override
    public <K, S, T, R> Observable<R> liveAggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R> aggregator) {
        return liveQuery(query.toBuilder().predicate(null).build())
            .throttleLatest(aggregationDebounceTime.toMillis(), TimeUnit.MILLISECONDS)
            .switchMapMaybe(n -> aggregate(query, aggregator))
            .distinctUntilChanged();
    }

    private <K, S, T> ObservableTransformer<Notification<S>, Notification<S>> applyReferencedObserve(QueryInfo<K, S, T> query) {
        QueryInfo<K, S, S> unmappedQuery = QueryInfos
                .unmapQuery(query)
                .toBuilder()
                .properties(QueryInfos.allReferencedProperties(query))
                .build();

        if (unmappedQuery.properties().isEmpty()) {
            return src -> src;
        }

        AtomicReference<Long> lastCreatedSequenceNumber = new AtomicReference<>();

        Map<PropertyExpression<S, S, ?>, ImmutableSet<PropertyExpression<S, ?, ?>>> propertiesByRoot = unmappedQuery
                .properties()
                .stream()
                .collect(Collectors.groupingBy(PropertyExpressions::rootOf, ImmutableSet.toImmutableSet()));

        ObservableTransformer<Notification<S>, Notification<S>> transformer = propertiesByRoot.keySet()
                .stream()
                .filter(PropertyExpressions::isReference)
                .map(p -> observeReferenceProperty(unmappedQuery, p, propertiesByRoot.get(p), lastCreatedSequenceNumber))
                .reduce(Observable::mergeWith)
                .<ObservableTransformer<Notification<S>, Notification<S>>>map(o -> src -> src.mergeWith(o))
                .orElse(src -> src);

        return src -> src
                .compose(transformer)
                .doOnNext(n -> Optional
                        .of(n)
                        .filter(Notification::isCreate)
                        .map(Notification::sequenceNumber)
                        .ifPresent(lastCreatedSequenceNumber::set));
    }

    @SuppressWarnings("UnstableApiUsage")
    private <K1, S1, S2> Observable<Notification<S1>> observeReferenceProperty(QueryInfo<K1, S1, S1> query, PropertyExpression<S1, S1, S2> referenceProperty, ImmutableSet<PropertyExpression<S1, ?, ?>> properties, AtomicReference<Long> lastCreatedSequenceNumber) {
        ImmutableSet<PropertyExpression<S2, ?, ?>> referenceProperties = properties.stream()
                .filter(p -> !Objects.equals(p, referenceProperty))
                .map(p -> PropertyExpressions.tailOf(p, referenceProperty))
                .collect(ImmutableSet.toImmutableSet());
        return observeReferenceProperty(query, referenceProperty, referenceProperties, MetaClasses.forTokenWithKeyUnchecked(referenceProperty.reflect().objectType()), lastCreatedSequenceNumber);
    }

    @SuppressWarnings({"unchecked", "ReactiveStreamsNullableInLambdaInTransform"})
    private <K1, S1, K2, S2> Observable<Notification<S1>> observeReferenceProperty(QueryInfo<K1, S1, S1> query, PropertyExpression<S1, S1, S2> referenceProperty, ImmutableSet<PropertyExpression<S2, ?, ?>> properties, MetaClassWithKey<K2, S2> metaClassWithKey, AtomicReference<Long> lastCreatedSequenceNumber) {
        return observeReference(metaClassWithKey, properties)
                .filter(n -> n.isModify() || n.isDelete())
                .doOnNext(n -> log.trace("Received referenced notification: {} (last seq.: {}), {}", n.sequenceNumber(), lastCreatedSequenceNumber.get(), n))
                .flatMapIterable(n -> query(QueryInfo
                        .<K1, S1, S1>builder()
                        .metaClass(query.metaClass())
                        .properties(PropertyExpressions.includeMandatoryProperties(query.objectType(), query.properties()))
                        .predicate(PredicateBuilder.<S1>create()
                                //.and(query.predicate())
                                .and(matchReferenceId(n.oldValue(), referenceProperty, metaClassWithKey))
                                .and(matchSequenceNumber(query.metaClass(), lastCreatedSequenceNumber.get(), n.sequenceNumber()))
                                .build())
                        .build())
                        .map(Notification::newValue)
                        .map(obj -> {
                            MetaBuilder<S1> builder = ((HasMetaClass<S1>)obj).toBuilder();
                            referenceProperty.property().setValue(builder, n.oldValue());
                            S1 oldValue = builder.build();
                            referenceProperty.property().setValue(builder, n.newValue());
                            S1 newValue = builder.build();
                            return Notification.create(oldValue, newValue, n.sequenceNumber());
                        })
                        .blockingIterable());
    }

    @SuppressWarnings("UnstableApiUsage")
    private <S> ObjectExpression<S, Boolean> matchSequenceNumber(MetaClass<S> sourceMeta, @Nullable Long lastCreatedSeqNum, @Nullable Long notificationSeqNum) {
        Long seqNum = lastCreatedSeqNum != null && notificationSeqNum != null
                ? Long.valueOf(Math.max(lastCreatedSeqNum, notificationSeqNum))
                : lastCreatedSeqNum != null ? lastCreatedSeqNum : notificationSeqNum;

        return Optional
                .ofNullable(seqNum)
                .<ObjectExpression<S, Boolean>>map(sn -> NumericUnaryOperationExpression.<S, S, Long>create(Expression.Type.SequenceNumber, ObjectExpression.objectArg(sourceMeta.asType()))
                        .lessOrEqual(sn))
                .orElse(null);
    }

    private <S, KT, T> ObjectExpression<S, Boolean> matchReferenceId(T referencedObject, PropertyExpression<S, S, T> referenceProperty, MetaClassWithKey<KT, T> metaClass) {
        return PropertyExpression
                .ofObject(referenceProperty, metaClass.keyProperty())
                .eq(metaClass.keyOf(referencedObject));
    }

    private <K, S> Observable<Notification<S>> observeReference(MetaClassWithKey<K, S> metaClass, ImmutableSet<PropertyExpression<S, ?, ?>> properties) {
        return liveQuery(QueryInfo.<K, S, S>builder()
                .metaClass(metaClass)
                .properties(properties)
                .build());
    }
}
