package com.slimgears.rxrepo.query.decorator;

import com.slimgears.rxrepo.expressions.Aggregator;
import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.query.provider.DeleteInfo;
import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.rxrepo.query.provider.UpdateInfo;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import io.reactivex.*;
import io.reactivex.functions.Function;
import io.reactivex.schedulers.Schedulers;

public class SchedulingQueryProviderDecorator extends AbstractQueryProviderDecorator {
    private final Scheduler updateScheduler;
    private final Scheduler queryScheduler;
    private final Scheduler notificationScheduler;

    private SchedulingQueryProviderDecorator(
            QueryProvider underlyingProvider,
            Scheduler updateScheduler,
            Scheduler queryScheduler,
            Scheduler notificationScheduler) {
        super(underlyingProvider);
        this.updateScheduler = updateScheduler;
        this.queryScheduler = queryScheduler;
        this.notificationScheduler = notificationScheduler;
    }

    public static QueryProvider.Decorator create(
            Scheduler updateScheduler,
            Scheduler queryScheduler,
            Scheduler notificationScheduler) {
        return provider -> new SchedulingQueryProviderDecorator(provider, updateScheduler, queryScheduler, notificationScheduler);
    }

    public static QueryProvider.Decorator createDefault() {
        return create(Schedulers.computation());
    }

    public static QueryProvider.Decorator create(Scheduler scheduler) {
        return create(scheduler, scheduler, Schedulers.from(Runnable::run));
    }

    @Override
    public <K, S> Completable insert(MetaClassWithKey<K, S> metaClass, Iterable<S> entities) {
        return super.insert(metaClass, entities).subscribeOn(updateScheduler);
    }

    @Override
    public <K, S> Maybe<S> insertOrUpdate(MetaClassWithKey<K, S> metaClass, K key, Function<Maybe<S>, Maybe<S>> entityUpdater) {
        return super.insertOrUpdate(metaClass, key, entityUpdater).subscribeOn(updateScheduler);
    }

    @Override
    public <K, S> Single<S> insertOrUpdate(MetaClassWithKey<K, S> metaClass, S entity) {
        return super.insertOrUpdate(metaClass, entity).subscribeOn(updateScheduler);
    }

    @Override
    public <K, S> Single<Integer> update(UpdateInfo<K, S> update) {
        return super.update(update).subscribeOn(updateScheduler);
    }

    @Override
    public <K, S> Single<Integer> delete(DeleteInfo<K, S> delete) {
        return super.delete(delete).subscribeOn(updateScheduler);
    }

    @Override
    public <K, S> Completable drop(MetaClassWithKey<K, S> metaClass) {
        return super.drop(metaClass).subscribeOn(updateScheduler);
    }

    @Override
    public Completable dropAll() {
        return super.dropAll().subscribeOn(updateScheduler);
    }

    @Override
    public <K, S, T> Observable<T> query(QueryInfo<K, S, T> query) {
        return super.query(query).subscribeOn(queryScheduler);
    }

    @Override
    public <K, S, T, R> Maybe<R> aggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R> aggregator) {
        return super.aggregate(query, aggregator).subscribeOn(queryScheduler);
    }

    @Override
    public <K, S, T> Observable<Notification<T>> liveQuery(QueryInfo<K, S, T> query) {
        return super.liveQuery(query).subscribeOn(notificationScheduler);
    }

    @Override
    public <K, S, T, R> Observable<R> liveAggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R> aggregator) {
        return super.liveAggregate(query, aggregator).subscribeOn(notificationScheduler);
    }
}
