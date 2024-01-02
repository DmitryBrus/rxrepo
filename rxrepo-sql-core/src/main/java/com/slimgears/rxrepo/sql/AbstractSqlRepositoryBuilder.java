package com.slimgears.rxrepo.sql;

import com.slimgears.rxrepo.query.Repository;
import com.slimgears.rxrepo.query.RepositoryConfig;

public abstract class AbstractSqlRepositoryBuilder<_B extends AbstractSqlRepositoryBuilder<_B>> extends AbstractRepositoryBuilder<_B> {
    protected abstract SqlServiceFactory.Builder<?> serviceFactoryBuilder(RepositoryConfig config);

    protected AbstractSqlRepositoryBuilder() {
        this
                .bufferDebounceTimeoutMillis(1000)
                .aggregationDebounceTimeMillis(2000)
                .retryCount(5)
                .retryInitialDurationMillis(10);
    }

    public Repository build() {
        RepositoryConfig config = configBuilder.build();
        return serviceFactoryBuilder(config)
                .buildRepository(config);
    }
}
