package com.slimgears.rxrepo.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.slimgears.rxrepo.expressions.*;
import com.slimgears.rxrepo.expressions.internal.CollectionPropertyExpression;
import com.slimgears.rxrepo.filters.Filter;
import com.slimgears.rxrepo.query.provider.*;
import com.slimgears.util.autovalue.annotations.HasMetaClass;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import com.slimgears.util.rx.Observables;
import io.reactivex.*;
import io.reactivex.functions.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@SuppressWarnings("UnstableApiUsage")
public class DefaultEntitySet<K, S> implements EntitySet<K, S> {
    private final static Logger log = LoggerFactory.getLogger(DefaultEntitySet.class);
    private final QueryProvider queryProvider;
    private final MetaClassWithKey<K, S> metaClass;
    private final RepositoryConfigModel config;

    private DefaultEntitySet(QueryProvider queryProvider,
                             MetaClassWithKey<K, S> metaClass,
                             RepositoryConfigModel config) {
        this.queryProvider = queryProvider;
        this.metaClass = metaClass;
        this.config = config;
    }

    static <K, S> DefaultEntitySet<K, S> create(
            QueryProvider queryProvider,
            MetaClassWithKey<K, S> metaClass,
            RepositoryConfigModel config) {
        return new DefaultEntitySet<>(queryProvider, metaClass, config);
    }

    @Override
    public MetaClassWithKey<K, S> metaClass() {
        return metaClass;
    }

    @Override
    public EntityDeleteQuery<S> delete() {
        return new EntityDeleteQuery<S>() {
            private final AtomicReference<ObjectExpression<S, Boolean>> predicate = new AtomicReference<>();
            private final DeleteInfo.Builder<K, S> builder = DeleteInfo.builder();

            @Override
            public Single<Integer> execute() {
                return queryProvider.delete(builder
                        .metaClass(metaClass)
                        .predicate(predicate.get())
                        .build());
            }

            @Override
            public EntityDeleteQuery<S> where(ObjectExpression<S, Boolean> predicate) {
                updatePredicate(this.predicate, predicate);
                return this;
            }

            @Override
            public EntityDeleteQuery<S> limit(long limit) {
                builder.limit(limit);
                return this;
            }

            @Override
            public EntityDeleteQuery<S> where(Filter<S> filter) {
                return Optional.ofNullable(filter)
                        .flatMap(f -> f.<S>toExpression(metaClass.asType()))
                        .map(this::where)
                        .orElse(this);
            }
        };
    }

    @Override
    public EntityUpdateQuery<S> update() {
        return new EntityUpdateQuery<S>() {
            private final AtomicReference<ObjectExpression<S, Boolean>> predicate = new AtomicReference<>();
            private final UpdateInfo.Builder<K, S> builder = UpdateInfo.<K, S>builder().metaClass(metaClass);

            @Override
            public <T extends HasMetaClass<T>, V> EntityUpdateQuery<S> set(PropertyExpression<S, T, V> property, ObjectExpression<S, V> value) {
                builder.propertyUpdatesBuilder().add(PropertyUpdateInfo.create(property, value));
                return this;
            }

            @Override
            public <T extends HasMetaClass<T>, V, C extends Collection<V>> EntityUpdateQuery<S> add(CollectionPropertyExpression<S, T, V, C> property, ObjectExpression<S, V> item) {
                return collectionOperation(property, item, CollectionPropertyUpdateInfo.Operation.Add);
            }

            @Override
            public <T extends HasMetaClass<T>, V, C extends Collection<V>> EntityUpdateQuery<S> remove(CollectionPropertyExpression<S, T, V, C> property, ObjectExpression<S, V> item) {
                return collectionOperation(property, item, CollectionPropertyUpdateInfo.Operation.Remove);
            }

            @Override
            public Single<Integer> execute() {
                return Single
                        .defer(() -> queryProvider.update(builder
                                .predicate(predicate.get())
                                .build()));
            }

            @Override
            public EntityUpdateQuery<S> where(ObjectExpression<S, Boolean> predicate) {
                updatePredicate(this.predicate, predicate);
                return this;
            }

            @Override
            public EntityUpdateQuery<S> limit(long limit) {
                builder.limit(limit);
                return this;
            }

            @Override
            public EntityUpdateQuery<S> where(Filter<S> filter) {
                return Optional.ofNullable(filter)
                        .flatMap(f -> f.<S>toExpression(metaClass.asType()))
                        .map(this::where)
                        .orElse(this);
            }

            private <T extends HasMetaClass<T>, V, C extends Collection<V>> EntityUpdateQuery<S> collectionOperation(CollectionPropertyExpression<S, T, V, C> property, ObjectExpression<S, V> item, CollectionPropertyUpdateInfo.Operation operation) {
                builder.collectionPropertyUpdatesBuilder()
                        .add(CollectionPropertyUpdateInfo.create(property, item, operation));
                return this;
            }
        };
    }

    @Override
    public SelectQueryBuilder<S> query() {
        return new SelectQueryBuilder<S>() {
            private final ImmutableList.Builder<SortingInfo<S, ?, ? extends Comparable<?>>> sortingInfos = ImmutableList.builder();
            private final AtomicReference<ObjectExpression<S, Boolean>> predicate = new AtomicReference<>();
            private Long limit;
            private Long skip;

            @Override
            public <V extends Comparable<V>> SelectQueryBuilder<S> orderBy(PropertyExpression<S, ?, V> field, boolean ascending) {
                sortingInfos.add(SortingInfo.create(field, ascending));
                return this;
            }

            @Override
            public SelectQuery<S> select() {
                return select(ObjectExpression.arg(metaClass.asType()));
            }

            @Override
            public <T> SelectQuery<T> select(ObjectExpression<S, T> expression, boolean distinct) {
                return new SelectQuery<T>() {
                    private final QueryInfo.Builder<K, S, T> builder = QueryInfo.<K, S, T>builder()
                            .metaClass(metaClass)
                            .predicate(predicate.get())
                            .limit(limit)
                            .skip(skip)
                            .sorting(sortingInfos.build())
                            .mapping(omitEmptyMapping(expression))
                            .distinct(distinct);

                    @SuppressWarnings("ReactiveStreamsNullableInLambdaInTransform")
                    @Override
                    public Maybe<T> first() {
                        QueryInfo<K, S, T> query = builder.limit(1L).build();
                        return queryProvider.query(query).map(Notification::newValue).singleElement();
                    }

                    @Override
                    public <R> Maybe<R> aggregate(Aggregator<T, T, R> aggregator) {
                        return queryProvider.aggregate(builder.build(), aggregator);
                    }

                    @Override
                    public SelectQuery<T> properties(Iterable<PropertyExpression<T, ?, ?>> properties) {
                        builder.propertiesAddAll(properties);
                        return this;
                    }

                    @SuppressWarnings("ReactiveStreamsNullableInLambdaInTransform")
                    @Override
                    public Observable<T> retrieve() {
                        return queryProvider
                                .query(builder.build())
                                .map(Notification::newValue);
                    }
                };
            }

            @Override
            public LiveSelectQuery<S> liveSelect() {
                return liveSelect(ObjectExpression.arg(metaClass.asType()));
            }

            @Override
            public <T> LiveSelectQuery<T> liveSelect(ObjectExpression<S, T> expression) {
                return new LiveSelectQuery<T>() {
                    private final QueryInfo.Builder<K, S, T> builder = QueryInfo.<K, S, T>builder()
                            .metaClass(metaClass)
                            .predicate(predicate.get())
                            .mapping(omitEmptyMapping(expression));

                    @SuppressWarnings("ReactiveStreamsNullableInLambdaInTransform")
                    @Override
                    public Observable<T> first() {
                        QueryInfo<K, S, T> query = builder.limit(1L).build();
                        return queryProvider
                                .liveQuery(query)
                                .switchMapMaybe(n -> queryProvider.query(query)
                                        .map(Notification::newValue)
                                        .singleElement());
                    }

                    @Override
                    public LiveSelectQuery<T> properties(Iterable<PropertyExpression<T, ?, ?>> properties) {
                        builder.propertiesAddAll(properties);
                        return this;
                    }

                    @Override
                    public <R> Observable<R> aggregate(Aggregator<T, T, R> aggregator) {
                        QueryInfo<K, S, T> query = builder.build();
                        return queryProvider.liveAggregate(query, aggregator)
                                .mergeWith(queryProvider.aggregate(query, aggregator).toObservable())
                                .distinctUntilChanged();
                    }

                    @Override
                    public <R> Observable<R> observeAs(QueryTransformer<T, R> queryTransformer) {
                        QueryInfo<K, S, T> sourceQuery = builder.build();

                        QueryInfo<K, S, S> observeQuery = QueryInfos
                                .unmapQuery(sourceQuery)
                                .toBuilder()
                                .propertiesAddAll(QueryInfos.allReferencedProperties(sourceQuery))
                                .build();

                        QueryInfo<K, S, S> retrieveQuery = observeQuery.toBuilder()
                                .limit(limit)
                                .skip(skip)
                                .sortingAddAll(sortingInfos.build())
                                .build();

                        QueryInfo<K, S, T> transformQuery = sourceQuery.toBuilder()
                                .limit(limit)
                                .skip(skip)
                                .sortingAddAll(sortingInfos.build())
                                .build();

                        return queryProvider.aggregate(observeQuery, Aggregator.count())
                                .defaultIfEmpty(0L)
                                .map(AtomicLong::new)
                                .flatMapObservable(count -> {
                                    AtomicBoolean retrieveComplete = new AtomicBoolean();
                                    ObservableTransformer<List<Notification<S>>, R> transformer = queryTransformer
                                            .transformer(transformQuery, count);
                                    return queryProvider
                                            .queryAndObserve(retrieveQuery, observeQuery)
                                            .doOnNext(n -> log.trace("{}", Notifications.toBriefString(retrieveQuery.metaClass(), n)))
                                            .doOnNext(n -> {
                                                if (retrieveComplete.get()) updateCount(n, count);
                                            })
                                            .compose(Observables.bufferUntilIdle(Duration.ofMillis(config.bufferDebounceTimeoutMillis())))
                                            .filter(n -> !n.isEmpty())
                                            .map(l -> retrieveComplete.get()
                                                    ? l
                                                    : l.stream()
                                                    .filter(n -> {
                                                        if (n.isEmpty()) {
                                                            retrieveComplete.set(true);
                                                            return false;
                                                        }
                                                        return true;
                                                    })
                                                    .collect(Collectors.toList()))
                                            .compose(transformer);
                                });
                    }

                    private void updateCount(Notification<S> notification, AtomicLong count) {
                        if (notification.isDelete()) {
                            count.decrementAndGet();
                        } else if (notification.isCreate()) {
                            count.incrementAndGet();
                        }
                    }

                    @Override
                    public Observable<Notification<T>> queryAndObserve() {
                        QueryInfo<K, S, T> query = builder.build();
                        return queryProvider
                                .queryAndObserve(query.toBuilder().limit(limit).skip(skip).build(), query)
                                .filter(n -> !n.isEmpty())
                                .doOnNext(n -> log.trace("{}", Notifications.toBriefString(metaClass, n)));
                    }

                    @Override
                    public Observable<Notification<T>> observe() {
                        return queryProvider.liveQuery(builder.build());
                    }
                };
            }

            @Override
            public SelectQueryBuilder<S> where(ObjectExpression<S, Boolean> predicate) {
                updatePredicate(this.predicate, predicate);
                return this;
            }

            @Override
            public SelectQueryBuilder<S> limit(long limit) {
                this.limit = limit;
                return this;
            }

            @Override
            public SelectQueryBuilder<S> where(Filter<S> filter) {
                return Optional.ofNullable(filter)
                        .flatMap(f -> f.<S>toExpression(metaClass.asType()))
                        .map(this::where)
                        .orElse(this);
            }

            @Override
            public SelectQueryBuilder<S> skip(long skip) {
                this.skip = skip;
                return this;
            }
        };
    }

    @Override
    public Single<Supplier<S>> update(S entity) {
        return update(entity, true);
    }

    @Override
    public Single<Supplier<S>> updateNonRecursive(S entity) {
        return update(entity, false);
    }

    private Single<Supplier<S>> update(S entity, boolean recursive) {
        return queryProvider.insertOrUpdate(metaClass, entity, recursive);
    }

    @Override
    public Completable update(Iterable<S> entities) {
        return update(entities, true);
    }

    @Override
    public Completable updateNonRecursive(Iterable<S> entities) {
        return update(entities, false);
    }

    private Completable update(Iterable<S> entities, boolean recursive) {
        return Iterables.isEmpty(entities)
                ? Completable.complete()
                : queryProvider.insertOrUpdate(metaClass, entities, recursive);
    }

    @Override
    public Maybe<Supplier<S>> update(K key, Function<Maybe<S>, Maybe<S>> updater) {
        return update(key, updater, true);
    }

    @Override
    public Maybe<Supplier<S>> updateNonRecursive(K key, Function<Maybe<S>, Maybe<S>> updater) {
        return update(key, updater, false);
    }

    private Maybe<Supplier<S>> update(K key, Function<Maybe<S>, Maybe<S>> updater, boolean recursive) {
        Function<Maybe<S>, Maybe<S>> filteredUpdater = maybe -> {
            AtomicReference<S> entity = new AtomicReference<>();
            return updater.apply(maybe.doOnSuccess(entity::set))
                    .filter(e -> !Objects.equals(entity.get(), e))
                    .switchIfEmpty(Maybe.fromCallable(entity::get));
        };
        return queryProvider.insertOrUpdate(metaClass, key, recursive, filteredUpdater);
    }

    private static <S> void updatePredicate(AtomicReference<ObjectExpression<S, Boolean>> current, ObjectExpression<S, Boolean> predicate) {
        current.updateAndGet(exp -> Optional
                .ofNullable(exp)
                .<ObjectExpression<S, Boolean>>map(ex -> BooleanExpression.and(ex, predicate))
                .orElse(predicate));
    }

    private static <S, T> ObjectExpression<S, T> omitEmptyMapping(ObjectExpression<S, T> expression) {
        return Optional.ofNullable(expression)
                .filter(m -> m.type().operationType() != Expression.OperationType.Argument)
                .orElse(null);
    }
}
