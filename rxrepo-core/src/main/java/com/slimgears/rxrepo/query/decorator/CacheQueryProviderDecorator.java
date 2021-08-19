package com.slimgears.rxrepo.query.decorator;

import com.slimgears.nanometer.MetricCollector;
import com.slimgears.nanometer.Metrics;
import com.slimgears.rxrepo.expressions.Aggregator;
import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import io.reactivex.Observable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings("unused")
public class CacheQueryProviderDecorator implements QueryProvider.Decorator {
    private final static Logger log = LoggerFactory.getLogger(CacheQueryProviderDecorator.class);
    private final static MetricCollector metrics = Metrics.collector(CacheQueryProviderDecorator.class);

    @Override
    public QueryProvider apply(QueryProvider queryProvider) {
        return new DecoratedProvider(queryProvider);
    }

    public static QueryProvider.Decorator create() {
        return new CacheQueryProviderDecorator();
    }

    private static class DecoratedProvider extends AbstractQueryProviderDecorator {
        private final Map<QueryInfo<?, ?, ?>, Observable<Notification<?>>> activeQueries = new ConcurrentHashMap<>();
        private final Map<QueryInfo<?, ?, ?>, Observable<Notification<?>>> activeAggregateQueries = new ConcurrentHashMap<>();

        private DecoratedProvider(QueryProvider underlyingProvider) {
            super(underlyingProvider);
        }

        @SuppressWarnings("unchecked")
        @Override
        public <K, S, T> Observable<Notification<T>> liveQuery(QueryInfo<K, S, T> query) {
            return (Observable<Notification<T>>)(Observable<?>)activeQueries
                    .computeIfAbsent(query, q -> (Observable<Notification<?>>)(Observable<?>)super
                            .liveQuery(query)
                            .doOnSubscribe(d -> {
                                log.debug("New subscription added. Currently active: {}. Currently subscribed query: {}", activeQueries.size(), query);
                            })
                            .doFinally(() -> {
                                activeQueries.remove(query);
                                log.debug("Subscription closed. Currently active: {}", activeQueries.size());
                            })
                            .share());
        }
    }
}
