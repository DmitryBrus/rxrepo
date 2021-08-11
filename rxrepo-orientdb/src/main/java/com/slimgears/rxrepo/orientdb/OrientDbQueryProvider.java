package com.slimgears.rxrepo.orientdb;

import com.google.common.base.Stopwatch;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Table;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.sequence.OSequence;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.rxrepo.sql.*;
import com.slimgears.rxrepo.util.SchedulingProvider;
import com.slimgears.util.autovalue.annotations.HasMetaClass;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClass;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import com.slimgears.util.stream.Optionals;
import com.slimgears.util.stream.Streams;
import io.reactivex.Completable;
import io.reactivex.Observable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ConcurrentModificationException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class OrientDbQueryProvider extends SqlQueryProvider {
    private final static Logger log = LoggerFactory.getLogger(OrientDbQueryProvider.class);
    private final OrientDbSessionProvider dbSessionProvider;
    private final int bufferSize;
    private final KeyEncoder keyEncoder;

    OrientDbQueryProvider(SqlStatementProvider statementProvider,
                          SqlStatementExecutor statementExecutor,
                          SchemaProvider schemaProvider,
                          ReferenceResolver referenceResolver,
                          SchedulingProvider schedulingProvider,
                          OrientDbSessionProvider dbSessionProvider,
                          KeyEncoder keyEncoder,
                          int bufferSize) {
        super(statementProvider, statementExecutor, schemaProvider, referenceResolver, schedulingProvider);
        this.dbSessionProvider = dbSessionProvider;
        this.bufferSize = bufferSize;
        this.keyEncoder = keyEncoder;
    }

    static OrientDbQueryProvider create(SqlServiceFactory serviceFactory, OrientDbSessionProvider sessionProvider, int bufferSize) {
        return new OrientDbQueryProvider(
                serviceFactory.statementProvider(),
                serviceFactory.statementExecutor(),
                serviceFactory.schemaProvider(),
                serviceFactory.referenceResolver(),
                serviceFactory.schedulingProvider(),
                sessionProvider,
                serviceFactory.keyEncoder(),
                bufferSize);
    }

    @Override
    public <K, S> Completable insert(MetaClassWithKey<K, S> metaClass, Iterable<S> entities, boolean recursive) {
        if (entities == null || Iterables.isEmpty(entities)) {
            return Completable.complete();
        }

        Stopwatch stopwatch = Stopwatch.createStarted();
        return schemaProvider.createOrUpdate(metaClass)
                .andThen(Observable.fromIterable(entities)
                        .buffer(bufferSize)
                        .concatMapCompletable(buffer -> Completable.fromAction(() -> createAndSaveElements(metaClass, buffer))))
                .doOnComplete(() -> log.debug("Total insert time: {}s", stopwatch.elapsed(TimeUnit.SECONDS)));
    }

    private <S> void createAndSaveElements(MetaClass<S> metaClass, Iterable<S> entities) {
        Table<MetaClass<?>, Object, OElement> queryCache = HashBasedTable.create();
        AtomicLong seqNum = new AtomicLong();
        dbSessionProvider.withSession(dbSession -> {
            try {
                dbSession.declareIntent(new OIntentMassiveInsert());
//                dbSession.begin();
                OSequence sequence = dbSession.getMetadata().getSequenceLibrary().getSequence(OrientDbSchemaProvider.sequenceName);
                seqNum.set(sequence.next());
                Streams.fromIterable(entities)
                        .map(entity -> toOrientDbObject(entity, queryCache, dbSession, seqNum.get()))
                        .forEach(OElement::save);
                log.debug("Saving {} objects ({} types)", queryCache.size(), queryCache.rowKeySet().size());
//                dbSession.commit();
            } catch (OConcurrentModificationException | ORecordDuplicatedException e) {
//                dbSession.rollback();
                throw new ConcurrentModificationException(e.getMessage(), e);
            } finally {
                dbSession.declareIntent(null);
            }
            //log.info("{} Written sequence num: {}", metaClass.simpleName(), seqNum.get());
        });
    }

    private <S> OElement toOrientDbObject(S entity, OrientDbObjectConverter converter) {
        return (OElement)converter.toOrientDbObject(entity);
    }

    private <S> OElement toOrientDbObject(S entity, Table<MetaClass<?>, Object, OElement> queryCache, ODatabaseDocument dbSession, long seqNum) {
        OElement oEl = toOrientDbObject(entity, OrientDbObjectConverter.create(
                meta -> {
                    OElement element = dbSession.newElement(schemaProvider.tableName(meta));
                    element.setProperty(sequenceNumField, seqNum);
                    return element;
                },
                (converter, hasMetaClass) -> {
                    Object key = keyOf(hasMetaClass);
                    MetaClass<?> metaClass = hasMetaClass.metaClass();
                    OElement oElement = Optionals.or(
                            () -> Optional.ofNullable(queryCache.get(metaClass, key)),
                            () -> queryDocument(hasMetaClass, queryCache, dbSession),
                            () -> Optional
                                    .ofNullable(converter.toOrientDbObject(hasMetaClass))
                                    .map(OElement.class::cast)
                                    .map(element -> {
                                        element.setProperty(sequenceNumField, seqNum);
                                        return element;
                                    }))
                            .orElse(null);
                    if (oElement != null) {
                        queryCache.put(metaClass, key, oElement);
                    }
                    return oElement;
                },
                keyEncoder));
        queryCache.put(((HasMetaClass)entity).metaClass(), entity, oEl);
        return oEl;
    }

    @SuppressWarnings("unchecked")
    private static <K, S> K keyOf(HasMetaClassWithKey<K, S> hasMetaClass) {
        return hasMetaClass.metaClass().keyOf((S)hasMetaClass);
    }

    private <K, S> Optional<OElement> queryDocument(HasMetaClassWithKey<K, S> entity, Table<MetaClass<?>, Object, OElement> queryCache, ODatabaseDocument dbSession) {
        MetaClassWithKey<K, S> metaClass = entity.metaClass();
        K keyValue = keyOf(entity);
        if (queryCache.contains(metaClass, keyValue)) {
            return Optional.of(queryCache.get(metaClass, keyValue));
        }
        SqlStatement statement = statementProvider.forQuery(QueryInfo.
                <K, S, S>builder()
                .metaClass(metaClass)
                .predicate(PropertyExpression.ofObject(metaClass.keyProperty()).eq(keyValue))
                .build());

        OResultSet queryResults = dbSession.query(statement.statement(), statement.args());
        Optional<OElement> existing = queryResults.elementStream().findAny();
        queryResults.close();
        if (existing.isPresent()) {
            queryCache.put(metaClass, keyValue, existing.get());
            return existing;
        }
        return Optional.empty();
    }
}
