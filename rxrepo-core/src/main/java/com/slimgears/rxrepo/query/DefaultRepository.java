package com.slimgears.rxrepo.query;

import com.slimgears.rxrepo.query.decorator.MandatoryPropertiesQueryProviderDecorator;
import com.slimgears.rxrepo.query.decorator.TakeUntilCloseQueryProviderDecorator;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import io.reactivex.Completable;
import io.reactivex.subjects.CompletableSubject;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

public class DefaultRepository implements Repository {
    public final static RepositoryConfigModel defaultConfig = RepositoryConfig
            .builder()
            .retryCount(10)
            .bufferDebounceTimeoutMillis(100)
            .aggregationDebounceTimeMillis(2000)
            .retryInitialDurationMillis(10)
            .build();

    private final RepositoryConfigModel config;
    private final QueryProvider queryProvider;
    private final Map<MetaClassWithKey<?, ?>, EntitySet<?, ?>> entitySetMap = new HashMap<>();

    DefaultRepository(QueryProvider queryProvider, RepositoryConfigModel config) {
        this.queryProvider = QueryProvider.Decorator.of(
                TakeUntilCloseQueryProviderDecorator.create(),
                MandatoryPropertiesQueryProviderDecorator.create())
                .apply(queryProvider);
        this.config = Optional.ofNullable(config).orElse(defaultConfig);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, T> EntitySet<K, T> entities(MetaClassWithKey<K, T> meta) {
        return (EntitySet<K, T>)entitySetMap.computeIfAbsent(meta, m -> createEntitySet(meta));
    }

    @Override
    public Iterable<EntitySet<?, ?>> allEntitySets() {
        return entitySetMap.values();
    }

    @Override
    public Completable clear() {
        return queryProvider.dropAll();
    }

    private <K, T> EntitySet<K, T> createEntitySet(MetaClassWithKey<K, T> metaClass) {
        return DefaultEntitySet.create(queryProvider, metaClass, config);
    }

    @Override
    public void close() {
        this.queryProvider.close();
    }
}
