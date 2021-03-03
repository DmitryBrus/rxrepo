package com.slimgears.rxrepo.query.decorator;

import com.slimgears.nanometer.ExecutorMetrics;
import com.slimgears.nanometer.MetricCollector;
import com.slimgears.rxrepo.expressions.Aggregator;
import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.query.provider.DeleteInfo;
import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.rxrepo.query.provider.UpdateInfo;
import com.slimgears.util.autovalue.annotations.MetaClass;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import io.reactivex.Completable;
import io.reactivex.Maybe;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.functions.Function;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class MetricsQueryProviderDecorator implements QueryProvider.Decorator, MetricCollector.Binder, AutoCloseable {
    private final AtomicReference<MetricCollector> metricCollector;

    private MetricsQueryProviderDecorator(MetricCollector collector) {
        metricCollector = new AtomicReference<>(collector.name("rxrepo"));
    }

    public static MetricsQueryProviderDecorator create() {
        return new MetricsQueryProviderDecorator(MetricCollector.empty());
    }

    public static MetricsQueryProviderDecorator create(MetricCollector collector) {
        return new MetricsQueryProviderDecorator(collector);
    }

    public java.util.function.Function<Executor, Executor> executorDecorator() {
        return this::decorateExecutor;
    }

    private Executor decorateExecutor(Executor executor) {
        return ExecutorMetrics.wrap(executor, metricCollector.get().name("scheduler"));
    }

    @Override
    public void bindTo(MetricCollector.Factory factory) {
        metricCollector.set(factory.name("rxrepo").create());
    }

    @Override
    public void close() {
        metricCollector.set(MetricCollector.empty());
    }

    @Override
    public QueryProvider apply(QueryProvider queryProvider) {
        return new Decorator(queryProvider);
    }

    class Decorator extends AbstractQueryProviderDecorator {
        protected Decorator(QueryProvider underlyingProvider) {
            super(underlyingProvider);
        }

        @Override
        public <K, S> Completable insert(MetaClassWithKey<K, S> metaClass, Iterable<S> entities) {
            return super
                    .insert(metaClass, entities)
                    .compose(asyncCollector("insert", metaClass).forCompletable());
        }

        @Override
        public <K, S> Completable insertOrUpdate(MetaClassWithKey<K, S> metaClass, Iterable<S> entities) {
            return super
                    .insertOrUpdate(metaClass, entities)
                    .compose(asyncCollector("insertOrUpdate", metaClass).forCompletable());
        }

        @Override
        public <K, S> Maybe<Single<S>> insertOrUpdate(MetaClassWithKey<K, S> metaClass, K key, Function<Maybe<S>, Maybe<S>> entityUpdater) {
            return super.insertOrUpdate(metaClass, key, entityUpdater)
                    .compose(asyncCollector("insertOrUpdateAtomic", metaClass).forMaybe());
        }

        @Override
        public <K, S, T> Observable<Notification<T>> query(QueryInfo<K, S, T> query) {
            return super.query(query)
                    .compose(asyncCollector("query", query.metaClass()).forObservable());
        }

        @Override
        public <K, S, T> Observable<Notification<T>> liveQuery(QueryInfo<K, S, T> query) {
            return super.liveQuery(query)
                    .compose(asyncCollector("liveQuery", query.metaClass()).forObservable());
        }

        @Override
        public <K, S, T> Observable<Notification<T>> queryAndObserve(QueryInfo<K, S, T> queryInfo, QueryInfo<K, S, T> observeInfo) {
            return super.queryAndObserve(queryInfo, observeInfo)
                    .compose(asyncCollector("queryAndObserve", queryInfo.metaClass()).forObservable());
        }

        @Override
        public <K, S, T, R> Maybe<R> aggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R> aggregator) {
            return super.aggregate(query, aggregator)
                    .compose(asyncCollector("aggregate", query.metaClass()).forMaybe());
        }

        @Override
        public <K, S, T, R> Observable<R> liveAggregate(QueryInfo<K, S, T> query, Aggregator<T, T, R> aggregator) {
            return super.liveAggregate(query, aggregator)
                    .compose(asyncCollector("liveAggregate", query.metaClass()).forObservable());
        }

        @Override
        public <K, S> Single<Integer> update(UpdateInfo<K, S> update) {
            return super.update(update)
                    .compose(asyncCollector("batchUpdate", update.metaClass()).forSingle());
        }

        @Override
        public <K, S> Single<Integer> delete(DeleteInfo<K, S> delete) {
            return super.delete(delete)
                    .compose(asyncCollector("delete", delete.metaClass()).forSingle());
        }

        @Override
        public <K, S> Completable drop(MetaClassWithKey<K, S> metaClass) {
            return super.drop(metaClass)
                    .compose(asyncCollector("drop", metaClass).forCompletable());
        }

        private MetricCollector.Async asyncCollector(String operation, MetaClass<?> metaClass) {
            return metricCollector.get()
                    .name(metaClass.simpleName())
                    .name(operation)
                    .async()
                    .countSubscriptions("totalSubscriptionCount")
                    .countActiveSubscriptions("activeSubscriptionCount")
                    .countCompletions("completeCount")
                    .countErrors("errorCount")
                    .countItems("itemCount")
                    .timeTillFirst("timeTillFirst")
                    .timeTillComplete("timeTillComplete")
                    .timeBetweenItems("timeBetweenItems");
        }
    }
}
