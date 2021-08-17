package com.slimgears.rxrepo.orientdb;

import com.google.common.base.Stopwatch;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Table;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.intent.OIntent;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.sequence.OSequence;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.rxrepo.sql.*;
import com.slimgears.util.autovalue.annotations.*;
//import com.slimgears.rxrepo.util.SchedulingProvider;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClass;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import com.slimgears.util.generic.MoreStrings;
import com.slimgears.util.stream.Optionals;
import com.slimgears.util.stream.Streams;
import io.reactivex.Completable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.ConcurrentModificationException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.slimgears.rxrepo.orientdb.OrientDbSqlSchemaGenerator.sequenceName;

public class OrientDbQueryProvider extends DefaultSqlQueryProvider {
    private final static Logger log = LoggerFactory.getLogger(OrientDbQueryProvider.class);
    private final OrientDbSessionProvider dbSessionProvider;
    private final KeyEncoder keyEncoder;
    private final Cache<CacheKey, ORID> refCache = CacheBuilder.newBuilder()
            .initialCapacity(10000)
            .expireAfterAccess(Duration.ofMinutes(1))
            .concurrencyLevel(10)
            .build();

    static class CacheKey {
        private final MetaClassWithKey<?, ?> metaClass;
        private final Object key;

        CacheKey(MetaClassWithKey<?, ?> metaClass, Object key) {
            this.metaClass = metaClass;
            this.key = key;
        }

        public static CacheKey create(MetaClassWithKey<?, ?> metaClass, Object key) {
            return new CacheKey(metaClass, key);
        }

        @Override
        public int hashCode() {
            return Objects.hash(metaClass, key);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof CacheKey &&
                    Objects.equals(metaClass, ((CacheKey) obj).metaClass) &&
                    Objects.equals(key, ((CacheKey) obj).key);
        }
    }

    OrientDbQueryProvider(SqlStatementProvider statementProvider,
                          SqlStatementExecutor statementExecutor,
                          SqlSchemaGenerator schemaGenerator,
                          SqlReferenceResolver referenceResolver,
                          OrientDbSessionProvider dbSessionProvider,
                          KeyEncoder keyEncoder) {
        super(statementProvider, statementExecutor, schemaGenerator, referenceResolver);
        this.dbSessionProvider = dbSessionProvider;
        this.keyEncoder = keyEncoder;
    }

    static OrientDbQueryProvider create(SqlServiceFactory serviceFactory, OrientDbSessionProvider sessionProvider, int bufferSize) {
        return new OrientDbQueryProvider(
                serviceFactory.statementProvider(),
                serviceFactory.statementExecutor(),
                serviceFactory.schemaProvider(),
                serviceFactory.referenceResolver(),
                sessionProvider,
                serviceFactory.keyEncoder());
    }

    @Override
    public <K, S> Completable insert(MetaClassWithKey<K, S> metaClass, Iterable<S> entities, boolean recursive) {
        if (entities == null || Iterables.isEmpty(entities)) {
            return Completable.complete();
        }

        Stopwatch stopwatch = Stopwatch.createStarted();

        return schemaGenerator.createOrUpdate(metaClass)
                .andThen(dbSessionProvider.completeWithSession(session -> createAndSaveElements(session, entities, recursive)))
                .doOnComplete(() -> log.trace("Total insert time: {}s", stopwatch.elapsed(TimeUnit.SECONDS)));
    }

    private <S> Collection<OElement> createAndSaveElements(ODatabaseDocument dbSession, Iterable<S> entities, boolean recursive) {
        AtomicLong seqNum = new AtomicLong();
        Table<MetaClass<?>, Object, Object> cache = HashBasedTable.create();
        OIntent previousIntent = dbSession.getActiveIntent();
        try {
            dbSession.declareIntent(new OIntentMassiveInsert());
            dbSession.begin();
            OSequence sequence = dbSession.getMetadata().getSequenceLibrary().getSequence(sequenceName);
            seqNum.set(sequence.next());
            Map<CacheKey, OElement> elements = Streams.fromIterable(entities)
                    .collect(Collectors
                            .toMap(this::toCacheKey,
                                    entity -> toOrientDbObject(entity, cache, dbSession, seqNum.get(), recursive).save(),
                                    (a, b) -> b,
                                    LinkedHashMap::new));
            dbSession.commit();
            elements.forEach((key, value) -> refCache.put(key, value.getRecord().getIdentity()));
            return elements.values();
        } catch (OConcurrentModificationException | ORecordDuplicatedException e) {
            dbSession.rollback();
            throw new ConcurrentModificationException(e.getMessage(), e);
        } finally {
            dbSession.declareIntent(previousIntent);
        }
    }

    private <S> CacheKey toCacheKey(S entity) {
        return Optional.ofNullable(entity)
                .flatMap(Optionals.ofType(HasMetaClassWithKey.class))
                .map(k -> (HasMetaClassWithKey<?, ?>)k)
                .map(e -> CacheKey.create(e.metaClass(), keyOf(e)))
                .orElse(null);
    }

    private <S> OElement toOrientDbObject(S entity, OrientDbObjectConverter converter) {
        return (OElement)converter.toOrientDbObject(entity);
    }

    private <S> OElement toOrientDbObject(S entity, Table<MetaClass<?>, Object, Object> queryCache, ODatabaseDocument dbSession, long seqNum, boolean recursive) {
        OElement oEl = toOrientDbObject(entity, OrientDbObjectConverter.create(
                meta -> {
                    OElement element = dbSession.newElement(statementProvider.tableName((MetaClassWithKey<?, ?>) meta));
                    element.setProperty(SqlFields.sequenceFieldName, seqNum);
                    return element;
                },
                (converter, hasMetaClass) -> {
                    Object key = keyOf(hasMetaClass);
                    MetaClassWithKey<?, ?> metaClass = hasMetaClass.metaClass();
                    return Optionals.or(
                            () -> Optional.ofNullable(queryCache.get(metaClass, key)),
                            () -> Optional.ofNullable(refCache.getIfPresent(CacheKey.create(metaClass, key))),
                            () -> queryDocument(hasMetaClass, dbSession),
                            () -> {
                                if (recursive) {
                                    return Optional
                                            .ofNullable(converter.toOrientDbObject(hasMetaClass))
                                            .map(OElement.class::cast)
                                            .map(element -> {
                                                element.setProperty(SqlFields.sequenceFieldName, seqNum);
                                                queryCache.put(metaClass, key, element);
                                                return element;
                                            });
                                } else {
                                    throw new RuntimeException(MoreStrings.format("Could not find referenced object {}({})", metaClass.simpleName(), key));
                                }
                            })
                            .orElse(null);
                },
                keyEncoder));
        queryCache.put(((HasMetaClass<?>)entity).metaClass(), entity, oEl);
        return oEl;
    }

    @SuppressWarnings("unchecked")
    private static <K, S> K keyOf(S entity) {
        return Optional.ofNullable(entity)
                .flatMap(Optionals.ofType(HasMetaClassWithKey.class))
                .map(e -> (HasMetaClassWithKey<K, S>)e)
                .map(e -> e.metaClass().keyOf(entity))
                .orElse(null);
    }

    private <K, S> Optional<Object> queryDocument(HasMetaClassWithKey<K, S> entity, ODatabaseDocument dbSession) {
        MetaClassWithKey<K, S> metaClass = entity.metaClass();
        K keyValue = keyOf(entity);

        SqlStatement statement = statementProvider.forQuery(QueryInfo.
                <K, S, S>builder()
                .metaClass(metaClass)
                .predicate(PropertyExpression.ofObject(metaClass.keyProperty()).eq(keyValue))
                .build());

        statement = SqlStatement.create(
                statement.statement().replace("select ", "select @rid "),
                statement.args());

        OResultSet queryResults = dbSession.query(statement.statement(), statement.args());
        Optional<Object> existing = queryResults.stream().map(rs -> rs.getProperty("@rid")).findAny();
        queryResults.close();
        if (existing.isPresent()) {
            refCache.put(CacheKey.create(metaClass, keyValue), existing.map(ORID.class::cast).get());
        }
        return existing;
    }

    @Override
    public void close() {
        refCache.invalidateAll();
        refCache.cleanUp();
    }
}
