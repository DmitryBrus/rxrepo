package com.slimgears.rxrepo.query.decorator;

import com.slimgears.rxrepo.expressions.Aggregator;
import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import io.reactivex.Observable;
import io.reactivex.Scheduler;

public class ObserveOnSchedulingQueryProviderDecorator extends AbstractQueryProviderDecorator {
    private final Scheduler scheduler;

    private ObserveOnSchedulingQueryProviderDecorator(QueryProvider underlyingProvider, Scheduler scheduler) {
        super(underlyingProvider);
        this.scheduler = scheduler;
    }

    public static QueryProvider.Decorator create(Scheduler scheduler) {
        return src -> new ObserveOnSchedulingQueryProviderDecorator(src, scheduler);
    }

    @Override
    public <K, S, T> Observable<Notification<T>> queryAndObserve(QueryInfo<K, S, T> queryInfo, QueryInfo<K, S, T> observeInfo) {
        return super.queryAndObserve(queryInfo, observeInfo).observeOn(scheduler);
    }

    @Override
    public <K, S, T> Observable<Notification<T>> liveQuery(QueryInfo<K, S, T> query) {
        return super.liveQuery(query).observeOn(scheduler);
    }

    @Override
    public <K, S, T, R> Observable<R> liveAggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R> aggregator) {
        return super.liveAggregate(query, aggregator).observeOn(scheduler);
    }
}
