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
import io.reactivex.subjects.CompletableSubject;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

public class TakeUntilCloseQueryProviderDecorator implements QueryProvider.Decorator {
    private final static Object onCloseToken = new Object();

    private TakeUntilCloseQueryProviderDecorator() {
    }

    public static QueryProvider.Decorator create() {
        return new TakeUntilCloseQueryProviderDecorator();
    }

    @Override
    public QueryProvider apply(QueryProvider queryProvider) {
        return new Decorator(queryProvider);
    }

    static class Decorator extends AbstractQueryProviderDecorator {
        private final CompletableSubject closeSubject = CompletableSubject.create();
        private final Observable<Object> closeObservable = closeSubject
                .andThen(Observable.just(onCloseToken))
                .doOnNext(t -> log.info("Repository is being closed"))
                .share();

        private Decorator(QueryProvider underlyingProvider) {
            super(underlyingProvider);
        }

        @Override
        public <K, S, T, R> Maybe<R> aggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R> aggregator) {
            return super.aggregate(query, aggregator).takeUntil(closeObservable.firstElement());
        }

        @Override
        public <K, S, T> Observable<Notification<T>> query(QueryInfo<K, S, T> query) {
            return super.query(query).compose(applyTakeUntilClose());
        }

        @Override
        public <K, S, T> Observable<Notification<T>> liveQuery(QueryInfo<K, S, T> query) {
            return super.liveQuery(query).compose(applyTakeUntilClose());
        }

        @Override
        public <K, S, T> Observable<Notification<T>> queryAndObserve(QueryInfo<K, S, T> queryInfo, QueryInfo<K, S, T> observeInfo) {
            return super.queryAndObserve(queryInfo, observeInfo).compose(applyTakeUntilClose());
        }

        @Override
        public <K, S, T, R> Observable<R> liveAggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R> aggregator) {
            return super.liveAggregate(query, aggregator).compose(applyTakeUntilClose());
        }

        @Override
        public void close() {
            closeSubject.onComplete();
            super.close();
        }

        private <T> ObservableTransformer<T, T> applyTakeUntilClose() {
            return src -> src
                    .takeUntil(closeObservable.doOnNext(t -> log.debug("Stopping observable due to repository close")));
        }
    }
}
