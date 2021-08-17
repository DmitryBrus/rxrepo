package com.slimgears.rxrepo.query.decorator;

import com.google.common.reflect.TypeToken;
import com.slimgears.rxrepo.expressions.internal.MoreTypeTokens;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.rxrepo.util.PropertyMetas;
import com.slimgears.util.autovalue.annotations.*;
import com.slimgears.util.stream.Optionals;
import com.slimgears.util.stream.Streams;
import io.reactivex.Completable;
import io.reactivex.Maybe;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.functions.Function;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuppressWarnings({"UnstableApiUsage"})
public class UpdateReferencesFirstQueryProviderDecorator extends AbstractQueryProviderDecorator {
    protected UpdateReferencesFirstQueryProviderDecorator(QueryProvider underlyingProvider) {
        super(underlyingProvider);
    }

    public static QueryProvider.Decorator create() {
        return UpdateReferencesFirstQueryProviderDecorator::new;
    }

    @Override
    public <K, S> Completable insert(MetaClassWithKey<K, S> metaClass, Iterable<S> entities, boolean recursive) {
        return recursive
                ? insertReferences(metaClass, entities).andThen(super.insert(metaClass, entities, false))
                : super.insert(metaClass, entities, false);
    }

    @Override
    public <K, S> Completable insertOrUpdate(MetaClassWithKey<K, S> metaClass, Iterable<S> entities, boolean recursive) {
        return recursive
                ? insertReferences(metaClass, entities).andThen(super.insertOrUpdate(metaClass, entities, false))
                : super.insertOrUpdate(metaClass, entities, false);
    }

    @Override
    public <K, S> Single<Supplier<S>> insertOrUpdate(MetaClassWithKey<K, S> metaClass, S entity, boolean recursive) {
        return recursive
                ? insertReferences(metaClass, entity).andThen(super.insertOrUpdate(metaClass, entity, false))
                : super.insertOrUpdate(metaClass, entity, false);
    }

    @Override
    public <K, S> Maybe<Supplier<S>> insertOrUpdate(MetaClassWithKey<K, S> metaClass, K key, boolean recursive, Function<Maybe<S>, Maybe<S>> entityUpdater) {
        return recursive
                ? super.insertOrUpdate(metaClass, key, false, maybeEntity -> entityUpdater
                .apply(maybeEntity)
                .flatMap(updatedEntity -> insertReferences(metaClass, updatedEntity).andThen(Maybe.just(updatedEntity))))
                : super.insertOrUpdate(metaClass, key, false, entityUpdater);
    }

    private <K, S> Completable insertReferences(MetaClassWithKey<K, S> metaClass, S entity) {
        return insertReferences(metaClass, Collections.singleton(entity));
    }

    private <S> Completable insertReferences(MetaClassWithKey<?, S> metaClass, Iterable<S> entities) {
        return Observable.fromIterable(metaClass.properties())
                .flatMapCompletable(p -> insertReferences(p, entities));
    }

    private <K, S, T> Completable insertReferences(PropertyMeta<S, ?> property, Iterable<S> entities) {
        MetaClassWithKey<K, T> refMeta = this.<S, T>getReferenceType(property)
                .map(MetaClasses::<K, T>forTokenWithKeyUnchecked)
                .orElse(null);

        if (refMeta == null) {
            return Completable.complete();
        }

        Stream<S> entityStream = Streams.fromIterable(entities);
        Map<K, T> referencedObjects = this.<S, T>getReferences(property, entityStream)
                .filter(Objects::nonNull)
                .collect(Collectors.toMap(refMeta::keyOf, e -> e, (a, b) -> a));
        return referencedObjects.isEmpty()
                ? Completable.complete()
                : insertOrUpdate(refMeta, referencedObjects.values(), true);
    }

    @SuppressWarnings("unchecked")
    private <S, T> Optional<TypeToken<T>> getReferenceType(PropertyMeta<S, ?> property) {
        if (PropertyMetas.isReference(property)) {
            return Optional.of((TypeToken<T>)property.type());
        } else if (PropertyMetas.isReferenceCollection(property)) {
            return Optional.of(MoreTypeTokens.argType(property.type(), Collection.class));
        } else if (PropertyMetas.isReferenceMap(property)) {
            return Optional.of(MoreTypeTokens.argType(property.type(), Map.class, 1));
        }
        return Optional.empty();
    }

    @SuppressWarnings("unchecked")
    private <S, T> Stream<T> getReferences(PropertyMeta<S, ?> property, Stream<S> entities) {
        if (PropertyMetas.isReference(property)) {
            return getReferencesFromEntity((PropertyMeta<S, T>)property, entities);
        } else if (PropertyMetas.isReferenceCollection(property)) {
            return getReferenceFromCollection((PropertyMeta<S, Collection<T>>)property, entities);
        } else if (PropertyMetas.isReferenceMap(property)) {
            return getReferencesFromMap((PropertyMeta<S, Map<?, T>>)property, entities);
        }
        return Stream.empty();
    }

    private <S, T> Stream<T> getReferencesFromEntity(PropertyMeta<S, T> property, Stream<S> entities) {
        return entities.map(property::getValue);
    }

    private <K, S, T> Stream<T> getReferenceFromCollection(PropertyMeta<S, Collection<T>> property, Stream<S> entities) {
        return entities.map(property::getValue)
                .filter(Objects::nonNull)
                .flatMap(Collection::stream);
    }

    private <K, S, T> Stream<T> getReferencesFromMap(PropertyMeta<S, Map<?, T>> property, Stream<S> entities) {
        return entities.map(property::getValue)
                .filter(Objects::nonNull)
                .flatMap(v -> v.values().stream());
    }

    @SuppressWarnings("unchecked")
    private <K, S> K keyOf(S entity) {
        return Optional.ofNullable(entity)
                .flatMap(Optionals.ofType(HasMetaClassWithKey.class))
                .map(e -> (HasMetaClassWithKey<K, S>)e)
                .map(e -> e.metaClass().keyOf(entity))
                .orElse(null);
    }
}
