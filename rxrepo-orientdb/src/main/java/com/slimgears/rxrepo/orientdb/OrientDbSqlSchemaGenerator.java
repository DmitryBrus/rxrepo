package com.slimgears.rxrepo.orientdb;

import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.index.ORuntimeKeyIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.sequence.OSequence;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceLibrary;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.slimgears.rxrepo.annotations.Indexable;
import com.slimgears.rxrepo.sql.SqlSchemaGenerator;
import com.slimgears.rxrepo.sql.SqlFields;
import com.slimgears.rxrepo.util.PropertyMetas;
import com.slimgears.util.autovalue.annotations.*;
import com.slimgears.util.stream.Streams;
import io.reactivex.Completable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Optional;

@SuppressWarnings("UnstableApiUsage")
class OrientDbSqlSchemaGenerator implements SqlSchemaGenerator {
    private final static Logger log = LoggerFactory.getLogger(OrientDbSqlSchemaGenerator.class);
    private final OrientDbSessionProvider dbSessionProvider;
    private final Completable sequenceCreated;
    final static String sequenceName = "sequenceNum";


    OrientDbSqlSchemaGenerator(OrientDbSessionProvider sessionProvider) {
        this.dbSessionProvider = sessionProvider;
        this.sequenceCreated = Completable.fromAction(() -> dbSessionProvider.withSession(session -> {
                OSequenceLibrary sequenceLibrary = session.getMetadata().getSequenceLibrary();
                Optional
                        .ofNullable(sequenceLibrary.getSequence(sequenceName))
                        .orElseGet(() -> sequenceLibrary.createSequence(sequenceName, OSequence.SEQUENCE_TYPE.ORDERED, new OSequence.CreateParams()))
                        .save();
            })).cache();
    }

    @Override
    public Completable createDatabase() {
        return Completable.complete();
    }

    @Override
    public <K, T> Completable createOrUpdate(MetaClassWithKey<K, T> metaClass) {
//        return sequenceCreated.concatWith(dbSessionProvider.completeWithSession(dbSession -> createClass(dbSession, metaClass)));
        return sequenceCreated.concatWith(Completable.fromAction(() -> dbSessionProvider.withSession(session -> createClass(session, metaClass))));
    }

    @Override
    public void clear() {

    }

    @SuppressWarnings("rawtypes")
    private OClass createClass(ODatabaseDocument dbSession, MetaClass<?> metaClass) {
        String className = toClassName(metaClass);
        log.debug("Creating class: {}", className);
        OClass oClass = dbSession.createClassIfNotExist(className);

        int count = Iterables.size(metaClass.properties());
        if (count > 5) {
            oClass.setOverSize((float)count / 5);
        }

        Streams.fromIterable(metaClass.properties())
                .forEach(p -> {
                    log.trace(">> Adding property {}.{}", className, p.name());
                    try {
                        addProperty(dbSession, oClass, p);
                    } finally {
                        log.trace("<< Adding property {}.{}", className, p.name());
                    }
                });

        if (metaClass instanceof MetaClassWithKey) {
            log.trace("{}: Adding key index", className);
            MetaClassWithKey<?, ?> metaClassWithKey = (MetaClassWithKey<?, ?>) metaClass;

            OType oType = toOType(metaClassWithKey.keyProperty().type());
            if (oType.isLink()) {
                log.trace("{}: Adding reference key index", className);
                dbSession.getMetadata().getIndexManager().createIndex(
                        className + "." + metaClassWithKey.keyProperty().name() + "Index",
                        OClass.INDEX_TYPE.UNIQUE_HASH_INDEX.name(),
                        new ORuntimeKeyIndexDefinition<>(OLinkSerializer.ID),
                        null,
                        null,
                        null);
            } else {
                log.trace("{}: Adding simple key index", className);
                addIndex(oClass, metaClassWithKey.keyProperty(), true);
            }

            if (!oClass.existsProperty(SqlFields.sequenceFieldName)) {
                oClass.createProperty(SqlFields.sequenceFieldName, OType.LONG);
            }
        }

        log.trace("{}: Adding indexes for properties", className);
        Streams.fromIterable(metaClass.properties())
                .filter(p -> p.hasAnnotation(Indexable.class) && !p.hasAnnotation(Key.class))
                .forEach(p -> addIndex(oClass, p, p.getAnnotation(Indexable.class).unique()));

        log.trace("Class {} creation finished", className);

        return oClass;
    }

    private static void addIndex(OClass oClass, PropertyMeta<?, ?> propertyMeta, boolean unique) {
        log.trace(">> {}: Adding property {} index", oClass.getName(), propertyMeta.name());
        OClass.INDEX_TYPE indexType = unique ? OClass.INDEX_TYPE.UNIQUE_HASH_INDEX : OClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX;
        String propertyName = PropertyMetas.isEmbedded(propertyMeta)
                ? propertyMeta.name() + "AsString"
                : propertyMeta.name();
        addIndex(oClass, propertyName, indexType);
        log.trace("<< {}: Adding property {} index", oClass.getName(), propertyMeta.name());
    }

    private static void addIndex(OClass oClass, String propertyName, OClass.INDEX_TYPE indexType) {
        log.trace(">> {}: Adding property {} index of type {}", oClass.getName(), propertyName, indexType);
        if (!oClass.areIndexed(propertyName)) {
            oClass.createIndex(oClass.getName() + "." + propertyName + "Index", indexType, propertyName);
        }
        log.trace("<< {}: Adding property {} index of type {}", oClass.getName(), propertyName, indexType);
    }

    @SuppressWarnings("unchecked")
    private static <T extends HasMetaClass<T>> MetaClass<T> toMetaClass(TypeToken<?> typeToken) {
        return MetaClasses.forToken((TypeToken<T>)typeToken);
    }

    private <T> void addProperty(ODatabaseDocument dbSession, OClass oClass, PropertyMeta<T, ?> propertyMeta) {
        OType propertyOType = toOType(propertyMeta.type());
        log.trace("{}: Adding property {} of type {} ({})", oClass.getName(), propertyMeta.name(), propertyMeta.type().getRawType().getSimpleName(), propertyOType);

        if (propertyOType.isLink() || propertyOType.isEmbedded()) {
            OClass linkedOClass = dbSession.getClass(toClassName(propertyMeta.type()));
            if (oClass.existsProperty(propertyMeta.name())) {
                OProperty oProperty = oClass.getProperty(propertyMeta.name());
                if (oProperty.getType() != propertyOType) {
                    oProperty.setType(propertyOType);
                }
                if (!Objects.equals(oProperty.getLinkedClass(), linkedOClass)) {
                    oProperty.setLinkedClass(linkedOClass);
                }
            } else {
                oClass.createProperty(propertyMeta.name(), propertyOType, linkedOClass);
                if (PropertyMetas.isEmbedded(propertyMeta)) {
                    oClass.createProperty(propertyMeta.name() + "AsString", OType.STRING);
                }
            }
        } else {
            if (oClass.existsProperty(propertyMeta.name())) {
                OProperty oProperty = oClass.getProperty(propertyMeta.name());
                if (oProperty.getType() != propertyOType) {
                    oProperty.setType(propertyOType);
                }
            } else {
                oClass.createProperty(propertyMeta.name(), propertyOType);
            }
        }
    }

    private static OType toOType(TypeToken<?> token) {
        Class<?> cls = token.getRawType();
        if (PropertyMetas.isReference(token)) {
            return OType.LINK;
        } else if (PropertyMetas.isEmbedded(token)) {
            return OType.EMBEDDED;
        }
        return OType.getTypeByClass(cls);
    }

    private static String toClassName(MetaClass<?> metaClass) {
        return toClassName(metaClass.asType());
    }

    static String toClassName(TypeToken<?> cls) {
        return toClassName(cls.getRawType());
    }

    private static String toClassName(Class<?> cls) {
        return cls.getSimpleName();
    }
}
