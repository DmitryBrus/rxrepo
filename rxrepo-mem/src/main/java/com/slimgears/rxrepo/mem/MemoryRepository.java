package com.slimgears.rxrepo.mem;

import com.slimgears.rxrepo.query.Repository;
import com.slimgears.rxrepo.query.decorator.*;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import io.reactivex.schedulers.Schedulers;

import java.time.Duration;

public class MemoryRepository {
    public static Repository create(QueryProvider.Decorator... decorators) {
        return Repository
                .fromProvider(
                        MemoryQueryProvider.create(),
                        RetryOnConcurrentConflictQueryProviderDecorator.create(Duration.ofMillis(1), 5),
                        //LockQueryProviderDecorator.create(SemaphoreLockProvider.create()),
                        LiveQueryProviderDecorator.create(Duration.ofMillis(2000)),
                        ObserveOnSchedulingQueryProviderDecorator.create(Schedulers.io()),
                        SubscribeOnSchedulingQueryProviderDecorator.createDefault(),
                        UpdateReferencesFirstQueryProviderDecorator.create(),
                        QueryProvider.Decorator.of(decorators));
    }
}
