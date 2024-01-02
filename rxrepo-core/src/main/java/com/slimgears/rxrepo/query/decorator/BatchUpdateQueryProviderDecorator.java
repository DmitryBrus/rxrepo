package com.slimgears.rxrepo.query.decorator;

import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import io.reactivex.Completable;
import io.reactivex.Observable;
import io.reactivex.schedulers.Schedulers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ConcurrentModificationException;

public class BatchUpdateQueryProviderDecorator extends AbstractQueryProviderDecorator {
    private final int batchSize;
    private final static Logger log = LoggerFactory.getLogger(BatchUpdateQueryProviderDecorator.class);

    protected BatchUpdateQueryProviderDecorator(QueryProvider underlyingProvider, int batchSize) {
        super(underlyingProvider);
        this.batchSize = batchSize;
    }

    public static Decorator create(int batchSize) {
        return batchSize > 0
                ? src -> new BatchUpdateQueryProviderDecorator(src, batchSize)
                : Decorator.identity();
    }

    @Override
    public <K, S> Completable insertOrUpdate(MetaClassWithKey<K, S> metaClass, Iterable<S> entities, boolean recursive) {
        return Observable.fromIterable(entities)
                .buffer(batchSize)
                .flatMapCompletable(batch -> batchInsertOrUpdate(metaClass, batch, recursive)
                        .doOnSubscribe(d -> log.trace("[{}] <insertOrUpdate> batch of {} elements", metaClass.simpleName(), batch.size())));
    }

    private <K, S> Completable batchInsertOrUpdate(MetaClassWithKey<K, S> metaClass, Iterable<S> entities, boolean recursive) {
        return super.insert(metaClass, entities, recursive)
                .onErrorResumeNext(e -> e instanceof ConcurrentModificationException
                        ? Completable.defer(() -> super.insertOrUpdate(metaClass, entities, recursive)
                        .onErrorResumeNext(_e -> _e instanceof ConcurrentModificationException
                                ? Completable.defer(() -> Observable.fromIterable(entities)
                                .flatMapCompletable(entity -> insertOrUpdate(metaClass, entity, recursive).ignoreElement()))
                                : Completable.error(_e)))
                        : Completable.error(e));
    }

    @Override
    public <K, S> Completable insert(MetaClassWithKey<K, S> metaClass, Iterable<S> entities, boolean recursive) {
        return Observable.fromIterable(entities)
                .buffer(batchSize)
                .flatMapCompletable(batch -> super.insert(metaClass, batch, recursive)
                        .doOnSubscribe(d -> log.trace("[{}] <insert> batch of {} elements", metaClass.simpleName(), batch.size())));

    }
}
