package com.slimgears.rxrepo.sql;

import com.google.common.reflect.TypeToken;
import com.slimgears.rxrepo.util.PropertyMetas;
import com.slimgears.util.autovalue.annotations.MetaClass;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClasses;
import com.slimgears.util.stream.Optionals;
import io.reactivex.Completable;
import io.reactivex.Maybe;
import io.reactivex.Observable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

@SuppressWarnings("UnstableApiUsage")
public class CacheSqlSchemaGeneratorDecorator implements SqlSchemaGenerator {
    private final static Logger log = LoggerFactory.getLogger(CacheSqlSchemaGeneratorDecorator.class);
    private final SqlSchemaGenerator underlyingProvider;
    private final Map<String, Completable> cache = new ConcurrentHashMap<>();
    private final AtomicReference<Completable> dbCompletable = new AtomicReference<>();

    private CacheSqlSchemaGeneratorDecorator(SqlSchemaGenerator underlyingProvider) {
        this.underlyingProvider = underlyingProvider;
        this.clear();
    }

    public static SqlSchemaGenerator decorate(SqlSchemaGenerator schemaGenerator) {
        return new CacheSqlSchemaGeneratorDecorator(schemaGenerator);
    }

    @Override
    public Completable createDatabase() {
        return dbCompletable.get();
    }

    @Override
    public <K, T> Completable createOrUpdate(MetaClassWithKey<K, T> metaClass) {
        return cache.computeIfAbsent(
                metaClass.simpleName(),
                tn -> Completable.defer(() -> createOrUpdateWithReferences(metaClass)).cache());
    }

    private <K, T> Completable createOrUpdateWithReferences(MetaClassWithKey<K, T> metaClass) {
        log.trace("Creating meta class: {}", metaClass.simpleName());
        Completable references = referencedTypesOf(metaClass)
                .map(MetaClasses::forTokenWithKeyUnchecked)
                .concatMapCompletable(this::createOrUpdate);

        return references.andThen(underlyingProvider.createOrUpdate(metaClass));
    }

    private <T> Observable<TypeToken<?>> referencedTypesOf(MetaClass<T> metaClass) {
        return Observable
                .fromIterable(metaClass.properties())
                .<TypeToken<?>>flatMapMaybe(property -> Optionals.or(
                        () -> PropertyMetas.getReferencedType(property),
                        () -> PropertyMetas.isEmbedded(property) && PropertyMetas.isReference(property.type())
                                ? Optional.of(property.type())
                                : Optional.empty())
                        .map(Maybe::just)
                        .orElseGet(Maybe::empty)
                .filter(t -> !Objects.equals(t, metaClass.asType())));
    }

    @Override
    public void clear() {
        cache.clear();
        dbCompletable.set(Completable.defer(underlyingProvider::createDatabase).cache());
    }
}
