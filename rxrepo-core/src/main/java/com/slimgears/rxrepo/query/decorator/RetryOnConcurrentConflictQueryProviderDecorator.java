package com.slimgears.rxrepo.query.decorator;

import com.slimgears.rxrepo.query.provider.DeleteInfo;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.rxrepo.query.provider.UpdateInfo;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import com.slimgears.util.rx.Maybes;
import com.slimgears.util.rx.Singles;
import io.reactivex.Completable;
import io.reactivex.Maybe;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.exceptions.CompositeException;
import io.reactivex.functions.Function;

import java.time.Duration;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class RetryOnConcurrentConflictQueryProviderDecorator extends AbstractQueryProviderDecorator {
    private final Duration initialRetryDelay;
    private final int maxRetries;

    private RetryOnConcurrentConflictQueryProviderDecorator(QueryProvider underlyingProvider, Duration initialRetryDelay, int maxRetries) {
        super(underlyingProvider);
        this.initialRetryDelay = initialRetryDelay;
        this.maxRetries = maxRetries;
    }

    public static QueryProvider.Decorator create(Duration initialRetryDelay, int maxRetries) {
        return src -> new RetryOnConcurrentConflictQueryProviderDecorator(src, initialRetryDelay, maxRetries);
    }

    @Override
    public <K, S> Completable insert(MetaClassWithKey<K, S> metaClass, Iterable<S> entities, boolean recursive) {
        return super.insert(metaClass, entities, recursive)
                .onErrorResumeNext(e -> isConcurrencyException(e)
                        ? insertOrUpdate(metaClass, entities, recursive)
                        : Completable.error(e));
    }

    @Override
    public <K, S> Completable insertOrUpdate(MetaClassWithKey<K, S> metaClass, Iterable<S> entities, boolean recursive) {
        return super.insert(metaClass, entities, recursive)
                .onErrorResumeNext(e -> isConcurrencyException(e)
                        ? Completable.defer(() -> Observable
                        .fromIterable(entities)
                        .flatMapSingle(entity -> insertOrUpdate(metaClass, entity, recursive))
                        .ignoreElements())
                        : Completable.error(e));
    }

    @Override
    public <K, S> Single<Supplier<S>> insertOrUpdate(MetaClassWithKey<K, S> metaClass, S entity, boolean recursive) {
        return super.insert(metaClass, Collections.singleton(entity), recursive)
                .andThen(Single.<Supplier<S>>just(() -> entity))
                .onErrorResumeNext(e ->
                        isConcurrencyException(e)
                                ? Single.defer(() -> super.insertOrUpdate(metaClass, entity, recursive))
                                .compose(Singles.backOffDelayRetry(
                                        RetryOnConcurrentConflictQueryProviderDecorator::isConcurrencyException,
                                        initialRetryDelay,
                                        maxRetries))
                                : Single.error(e));
    }

    @Override
    public <K, S> Maybe<Supplier<S>> insertOrUpdate(MetaClassWithKey<K, S> metaClass, K key, boolean recursive, Function<Maybe<S>, Maybe<S>> entityUpdater) {
        AtomicInteger retry = new AtomicInteger();
        return Maybe.defer(() -> super.insertOrUpdate(metaClass, key, recursive, entityUpdater))
                .doOnSubscribe(d -> log.debug("{} [{}] Retry: {}", metaClass.simpleName(), key, retry.get()))
                .compose(Maybes.backOffDelayRetry(
                        RetryOnConcurrentConflictQueryProviderDecorator::isConcurrencyException,
                        initialRetryDelay,
                        maxRetries));
    }

    @Override
    public <K, S> Single<Integer> update(UpdateInfo<K, S> update) {
        return super.update(update)
                .compose(Singles.backOffDelayRetry(
                        RetryOnConcurrentConflictQueryProviderDecorator::isConcurrencyException,
                        initialRetryDelay,
                        maxRetries));
    }

    @Override
    public <K, S> Single<Integer> delete(DeleteInfo<K, S> delete) {
        return super.delete(delete)
                .compose(Singles.backOffDelayRetry(
                        RetryOnConcurrentConflictQueryProviderDecorator::isConcurrencyException,
                        initialRetryDelay,
                        maxRetries));
    }

    private static boolean isConcurrencyException(Throwable exception) {
        return exception instanceof ConcurrentModificationException ||
                exception instanceof NoSuchElementException ||
                (exception instanceof CompositeException && ((CompositeException)exception)
                        .getExceptions()
                        .stream()
                        .anyMatch(RetryOnConcurrentConflictQueryProviderDecorator::isConcurrencyException));
    }
}
