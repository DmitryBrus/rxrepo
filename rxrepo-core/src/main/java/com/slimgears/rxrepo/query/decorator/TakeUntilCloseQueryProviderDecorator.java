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
        private final Observable<Object> closeObservable = closeSubject.andThen(Observable.just(onCloseToken));
        private final AtomicBoolean wasClosed = new AtomicBoolean();

        private Decorator(QueryProvider underlyingProvider) {
            super(underlyingProvider);
        }
//
//        @Override
//        public <K, S> Single<Integer> update(UpdateInfo<K, S> update) {
//            return super.update(update).takeUntil(closeSubject);
//        }
//
//        @Override
//        public <K, S> Single<Integer> delete(DeleteInfo<K, S> delete) {
//            return super.delete(delete).takeUntil(closeSubject);
//        }
//
//        @Override
//        public <K, S> Completable insert(MetaClassWithKey<K, S> metaClass, Iterable<S> entities, boolean recursive) {
//            return super.insert(metaClass, entities, recursive).takeUntil(closeSubject);
//        }
//
//        @Override
//        public <K, S> Maybe<Supplier<S>> insertOrUpdate(MetaClassWithKey<K, S> metaClass, K key, boolean recursive, Function<Maybe<S>, Maybe<S>> entityUpdater) {
//            return super.insertOrUpdate(metaClass, key, recursive, entityUpdater).takeUntil(closeObservable.firstElement());
//        }
//
//        @Override
//        public <K, S> Single<Supplier<S>> insertOrUpdate(MetaClassWithKey<K, S> metaClass, S entity, boolean recursive) {
//            return super.insertOrUpdate(metaClass, entity, recursive).takeUntil(closeSubject);
//        }
//
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
            if (wasClosed.compareAndSet(false, true)) {
                closeSubject.onComplete();
                super.close();
            }
        }

        @Override
        protected QueryProvider getUnderlyingProvider() {
            return wasClosed.get() ? EmptyQueryProvider.instance : super.getUnderlyingProvider();
        }

        private <T> ObservableTransformer<T, T> applyTakeUntilClose() {
            return src -> src.takeUntil(closeSubject
                    .andThen(Observable.just(onCloseToken))
                    .doOnNext(o -> log.info("Closing repository")))
                    .doFinally(() -> log.info("Observable stopped"));
        }
    }
}
