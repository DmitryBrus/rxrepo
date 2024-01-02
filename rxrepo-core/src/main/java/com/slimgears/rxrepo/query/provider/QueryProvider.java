package com.slimgears.rxrepo.query.provider;

import com.slimgears.rxrepo.expressions.Aggregator;
import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.util.Queries;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClasses;
import io.reactivex.Completable;
import io.reactivex.Maybe;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.functions.Function;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

public interface QueryProvider extends AutoCloseable {
    <K, S> Maybe<Supplier<S>> insertOrUpdate(MetaClassWithKey<K, S> metaClass, K key, boolean recursive, Function<Maybe<S>, Maybe<S>> entityUpdater);
    <K, S, T> Observable<Notification<T>> query(QueryInfo<K, S, T> query);
    <K, S, T> Observable<Notification<T>> liveQuery(QueryInfo<K, S, T> query);
    <K, S, T, R> Maybe<R> aggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R> aggregator);

    <K, S> Single<Integer> update(UpdateInfo<K, S> update);
    <K, S> Single<Integer> delete(DeleteInfo<K, S> delete);
    <K, S> Completable drop(MetaClassWithKey<K, S> metaClass);
    Completable dropAll();

    default <K, S> Completable insert(MetaClassWithKey<K, S> metaClass, Iterable<S> entities, boolean recursive) {
        return insertOrUpdate(metaClass, entities, recursive);
    }

    default <K, S> Completable insertOrUpdate(MetaClassWithKey<K, S> metaClass, Iterable<S> entities, boolean recursive) {
        return Observable.fromIterable(entities)
                .concatMapEager(e -> insertOrUpdate(metaClass, e, recursive).toObservable())
                .ignoreElements();
    }

    default <K, S> Single<Supplier<S>> insertOrUpdate(MetaClassWithKey<K, S> metaClass, S entity, boolean recursive) {
        K key = metaClass.keyOf(entity);
        return insertOrUpdate(metaClass, key, recursive, val -> val
                .map(e -> MetaClasses.merge(metaClass, e, entity))
                .switchIfEmpty(Maybe.just(entity)))
                .toSingle();
    }

    default <K, S, T, R> Observable<R> liveAggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R> aggregator) {
        return liveQuery(query)
            .debounce(500, TimeUnit.MILLISECONDS)
            .switchMapMaybe(n -> aggregate(query, aggregator))
            .distinctUntilChanged();
    }

    default void close() {
    }

    @FunctionalInterface
    interface Decorator extends UnaryOperator<QueryProvider> {
        default Decorator andThen(Decorator decorator) {
            return qp -> decorator.apply(this.apply(qp));
        }

        static Decorator identity() {
            return qp -> qp;
        }

        static Decorator of(Decorator... decorators) {
            return Arrays.stream(decorators)
                    .reduce((first, second) -> first.andThen(second))
                    .orElseGet(Decorator::identity);
        }
    }

    default <K, S, T> Observable<Notification<T>> queryAndObserve(QueryInfo<K, S, T> queryInfo, QueryInfo<K, S, T> observeInfo) {
        return Queries.queryAndObserve(this.query(queryInfo), this.liveQuery(observeInfo));
    }
}
