package com.slimgears.rxrepo.query.decorator;

import com.slimgears.rxrepo.expressions.Aggregator;
import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.query.provider.DeleteInfo;
import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.rxrepo.query.provider.UpdateInfo;
import com.slimgears.rxrepo.util.Queries;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import io.reactivex.*;
import io.reactivex.functions.Function;
import io.reactivex.schedulers.Schedulers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

public class SubscribeOnSchedulingQueryProviderDecorator extends AbstractQueryProviderDecorator {
    private final static Logger log = LoggerFactory.getLogger(SubscribeOnSchedulingQueryProviderDecorator.class);
    private final Scheduler updateScheduler;
    private final Scheduler queryScheduler;
    private final Scheduler liveQueryScheduler;

    private SubscribeOnSchedulingQueryProviderDecorator(
            QueryProvider underlyingProvider,
            Scheduler updateScheduler,
            Scheduler queryScheduler,
            Scheduler liveQueryScheduler) {
        super(underlyingProvider);
        this.updateScheduler = updateScheduler;
        this.queryScheduler = queryScheduler;
        this.liveQueryScheduler = liveQueryScheduler;
    }

    public static QueryProvider.Decorator create(
            Scheduler updateScheduler,
            Scheduler queryScheduler,
            Scheduler notificationScheduler) {
        return provider -> new SubscribeOnSchedulingQueryProviderDecorator(provider, updateScheduler, queryScheduler, notificationScheduler);
    }

    public static QueryProvider.Decorator createDefault() {
        return create(Schedulers.computation(), Schedulers.io(), Schedulers.from(Runnable::run));
    }

    public static QueryProvider.Decorator create(Scheduler scheduler) {
        return create(scheduler, scheduler, Schedulers.from(Runnable::run));
    }

    @Override
    public <K, S> Completable insert(MetaClassWithKey<K, S> metaClass, Iterable<S> entities, boolean recursive) {
        return super.insert(metaClass, entities, recursive).subscribeOn(updateScheduler);
    }

    @Override
    public <K, S> Maybe<Supplier<S>> insertOrUpdate(MetaClassWithKey<K, S> metaClass, K key, boolean recursive, Function<Maybe<S>, Maybe<S>> entityUpdater) {
        return super.insertOrUpdate(metaClass, key, recursive, entityUpdater).subscribeOn(updateScheduler);
    }

    @Override
    public <K, S> Single<Supplier<S>> insertOrUpdate(MetaClassWithKey<K, S> metaClass, S entity, boolean recursive) {
        return super.insertOrUpdate(metaClass, entity, recursive).subscribeOn(updateScheduler);
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
    public <K, S, T> Observable<Notification<T>> query(QueryInfo<K, S, T> query) {
        return super.query(query).subscribeOn(queryScheduler);
    }

    @Override
    public <K, S, T, R> Maybe<R> aggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R> aggregator) {
        return super.aggregate(query, aggregator).subscribeOn(queryScheduler);
    }

    @Override
    public <K, S, T> Observable<Notification<T>> liveQuery(QueryInfo<K, S, T> query) {
        return super.liveQuery(query).subscribeOn(liveQueryScheduler);
    }

    @Override
    public <K, S, T> Observable<Notification<T>> queryAndObserve(QueryInfo<K, S, T> queryInfo, QueryInfo<K, S, T> observeInfo) {
        return super.queryAndObserve(queryInfo, observeInfo).subscribeOn(liveQueryScheduler);
    }

    @Override
    public <K, S, T, R> Observable<R> liveAggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R> aggregator) {
        return super.liveAggregate(query, aggregator).subscribeOn(liveQueryScheduler);
    }
}
