package com.slimgears.rxrepo.orientdb;

import com.google.common.base.Stopwatch;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Table;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.intent.OIntent;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.sequence.OSequence;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.slimgears.nanometer.MetricCollector;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.rxrepo.sql.*;
import com.slimgears.util.autovalue.annotations.HasMetaClass;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.slimgears.rxrepo.orientdb.OrientDbSqlSchemaGenerator.sequenceName;

public class OrientDbQueryProvider extends DefaultSqlQueryProvider {
    private final static Logger log = LoggerFactory.getLogger(OrientDbQueryProvider.class);
    private final OrientDbSessionProvider sessionProvider;
    private final KeyEncoder keyEncoder;
    private final LoadingCache<CacheKey<?, ?>, ORID> refCache;
    private final MetricCollector metricCollector;

    static class CacheKey<K, S> {
        private final MetaClassWithKey<K, S> metaClass;
        private final K key;

        CacheKey(MetaClassWithKey<K, S> metaClass, K key) {
            this.metaClass = metaClass;
            this.key = key;
        }

        public static <K, S> CacheKey<K, S> create(MetaClassWithKey<K, S> metaClass, K key) {
            return new CacheKey<>(metaClass, key);
        }

        public static <K, S> CacheKey<K, S> create(HasMetaClassWithKey<K, S> obj) {
            return new CacheKey<>(obj.metaClass(), keyOf(obj));
        }

        @Override
        public int hashCode() {
            return Objects.hash(metaClass, key);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof CacheKey &&
                    Objects.equals(metaClass, ((CacheKey<?, ?>) obj).metaClass) &&
                    Objects.equals(key, ((CacheKey<?, ?>) obj).key);
        }

        @Override
        public String toString() {
            return MoreStrings.format("[{}: {}]", metaClass.simpleName(), key);
        }
    }

    OrientDbQueryProvider(SqlServiceFactory serviceFactory,
                          OrientDbSessionProvider sessionProvider,
                          Duration cacheExpirationTime) {
        super(serviceFactory.statementProvider(),
                serviceFactory.statementExecutor(),
                serviceFactory.schemaProvider(),
                serviceFactory.referenceResolver());
        this.sessionProvider = sessionProvider;
        this.keyEncoder = serviceFactory.keyEncoder();
        this.metricCollector = serviceFactory.metricCollector().name("provider");
        this.refCache = CacheBuilder.newBuilder()
                .expireAfterAccess(cacheExpirationTime)
                .concurrencyLevel(10)
                .build(CacheLoader.from(this::queryReference));
    }

    public LoadingCache<CacheKey<?, ?>, ORID> getRefCache(MetaClass<?> metaClass) {
        return refCache;
    }

    static OrientDbQueryProvider create(SqlServiceFactory serviceFactory,
                                        OrientDbSessionProvider updateSessionProvider,
                                        Duration cacheExpirationTime) {
        return new OrientDbQueryProvider(
                serviceFactory,
                updateSessionProvider,
                cacheExpirationTime);
    }

    @Override
    public <K, S> Completable insert(MetaClassWithKey<K, S> metaClass, Iterable<S> entities, boolean recursive) {
        if (entities == null || Iterables.isEmpty(entities)) {
            return Completable.complete();
        }

        Stopwatch stopwatch = Stopwatch.createStarted();

        return schemaGenerator.useTable(metaClass)
                .andThen(sessionProvider.completeWithSession(session -> createAndSaveElements(session, metaClass, entities, recursive)))
                .doOnComplete(() -> log.trace("Total insert time: {}s", stopwatch.elapsed(TimeUnit.SECONDS)));
    }

    private <S> void createAndSaveElements(ODatabaseDocument dbSession, MetaClass<S> metaClass, Iterable<S> entities, boolean recursive) {
        OSchema schema = dbSession.getMetadata().getSchema();
        schema.reload();

        if (!schema.existsClass(metaClass.simpleName())) {
            throw new IllegalStateException(MoreStrings.format("Class {} not found", metaClass.simpleName()));
        }

        AtomicLong seqNum = new AtomicLong();
        OIntent previousIntent = dbSession.getActiveIntent();
        try {
            dbSession.declareIntent(new OIntentMassiveInsert());
            dbSession.begin();
            OSequence sequence = dbSession.getMetadata().getSequenceLibrary().getSequence(sequenceName);
            seqNum.set(sequence.next());
            Table<MetaClass<?>, Object, Object> cache = HashBasedTable.create();
            MetricCollector metrics = metricCollector.name(metaClass.simpleName());

            OrientDbObjectConverter objectConverter = OrientDbObjectConverter.create(
                    meta -> {
                        OElement element = dbSession.newElement(statementProvider.tableName((MetaClassWithKey<?, ?>) meta));
                        element.setProperty(SqlFields.sequenceFieldName, seqNum);
                        return element;
                    },
                    (converter, hasMetaClass) -> {
                        Object key = keyOf(hasMetaClass);
                        MetaClassWithKey<?, ?> _metaClass = hasMetaClass.metaClass();
                        MetricCollector.Timer.Stopper resolveTimeStopper = metrics.timer("resolveTime").stopper().start();
                        LoadingCache<CacheKey<?, ?>, ORID> refCache = getRefCache(_metaClass);
                        CacheKey<?, ?> refKey = CacheKey.create(hasMetaClass);
                        Object res = Optionals.or(
                                        () -> Optional.ofNullable(cache.get(_metaClass, key)),
                                        () -> {
                                            try {
                                                return Optional.ofNullable(refCache.get(refKey));
                                            } catch (CacheLoader.InvalidCacheLoadException e) {
                                                return Optional.empty();
                                            } catch (ExecutionException e) {
                                                throw new RuntimeException();
                                            }
                                        },
                                        () -> {
                                            if (recursive) {
                                                return Optional
                                                        .ofNullable(converter.toOrientDbObject(hasMetaClass))
                                                        .map(OElement.class::cast)
                                                        .map(element -> {
                                                            element.setProperty(SqlFields.sequenceFieldName, seqNum);
                                                            cache.put(_metaClass, key, element);
                                                            return element;
                                                        });
                                            } else {
                                                throw new RuntimeException(MoreStrings.format("Could not find referenced object {}({}) of {}", _metaClass.simpleName(), key, metaClass.simpleName()));
                                            }
                                        })
                                .orElse(null);
                        resolveTimeStopper.stop();
                        return res;
                    },
                    keyEncoder);

            MetricCollector.Timer.Stopper stopper = metrics.timer("convertTime").stopper().start();
            Stopwatch convertStopWatch = Stopwatch.createStarted();
            log.trace("Converting objects");
            Map<CacheKey, OElement> elements = Streams.fromIterable(entities)
                    .collect(Collectors
                            .toMap(this::toCacheKey,
                                    entity -> toOrientDbObject(entity, cache, objectConverter).save(),
                                    (a, b) -> b,
                                    LinkedHashMap::new));
            log.trace("Converted {} objects in {}s", elements.size(), convertStopWatch.elapsed(TimeUnit.SECONDS));
            stopper.stop();
            stopper = metrics.timer("commitTime").stopper().start();
            Stopwatch commitStopWatch = Stopwatch.createStarted();
            dbSession.commit();
            log.trace("Committed {} objects in {}s", elements.size(), commitStopWatch.elapsed(TimeUnit.SECONDS));
            stopper.stop();
            elements.forEach((key, value) -> getRefCache(metaClass).put(key, value.getRecord().getIdentity()));
        } catch (OConcurrentModificationException | ORecordDuplicatedException e) {
            dbSession.rollback();
            throw new ConcurrentModificationException(e.getMessage(), e);
        } finally {
            dbSession.declareIntent(previousIntent);
        }
    }

    private <S> CacheKey<?, ?> toCacheKey(S entity) {
        return Optional.ofNullable(entity)
                .flatMap(Optionals.ofType(HasMetaClassWithKey.class))
                .map(k -> (HasMetaClassWithKey<?, ?>)k)
                .map(e -> CacheKey.create(e.metaClass(), keyOf(e)))
                .orElse(null);
    }

    private <S> OElement toOrientDbObject(S entity, OrientDbObjectConverter converter) {
        return (OElement)converter.toOrientDbObject(entity);
    }

    private <S> OElement toOrientDbObject(S entity, Table<MetaClass<?>, Object, Object> queryCache, OrientDbObjectConverter objectConverter) {
        OElement oEl = toOrientDbObject(entity, objectConverter);
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

    private <K, S> Optional<ORID> queryReference(MetaClassWithKey<K, S> metaClass, K key, ODatabaseDocument dbSession) {
        SqlStatement statement = statementProvider.forQuery(QueryInfo.
                <K, S, S>builder()
                .metaClass(metaClass)
                .predicate(PropertyExpression.ofObject(metaClass.keyProperty()).eq(key))
                .build());
        statement = SqlStatement.create(
                statement.statement().replace("select ", "select @rid "),
                statement.args());

        OResultSet queryResults = dbSession.query(statement.statement(), statement.args());
        Optional<ORID> existing = queryResults.stream().map(rs -> rs.<ORID>getProperty("@rid")).findAny();
        queryResults.close();
        return existing;
    }

    private <K, S> ORID queryReference(CacheKey<K, S> cacheKey) {
        return sessionProvider
                .getWithSession(session -> queryReference(cacheKey.metaClass, cacheKey.key, session))
                .orElseThrow(() -> {
                    log.error("Could not find reference of {}[{}]", cacheKey.metaClass.simpleName(), cacheKey.key);
                    return new IllegalStateException(MoreStrings.format("Cannot find reference of {}[{}]", cacheKey.metaClass.simpleName(), cacheKey.key));
                });
    }

    @Override
    public void close() {
        refCache.invalidateAll();
        refCache.cleanUp();
    }
}
