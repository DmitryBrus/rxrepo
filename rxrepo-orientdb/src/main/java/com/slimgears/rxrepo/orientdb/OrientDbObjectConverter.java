package com.slimgears.rxrepo.orientdb;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.slimgears.rxrepo.sql.KeyEncoder;
import com.slimgears.rxrepo.util.PropertyMetas;
import com.slimgears.util.autovalue.annotations.HasMetaClass;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClass;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@SuppressWarnings("UnstableApiUsage")
class OrientDbObjectConverter {
    //private final static OrientDbObjectConverter instance = new OrientDbObjectConverter(meta -> new ODocument(), (converter, hasMetaClass) -> null);
    private final Function<MetaClass<?>, OElement> elementFactory;
    private final ElementResolver elementResolver;
    private final KeyEncoder keyEncoder;

    interface ElementResolver {
        Object resolve(OrientDbObjectConverter converter, HasMetaClassWithKey<?, ?> entity);
    }

    private OrientDbObjectConverter(Function<MetaClass<?>, OElement> elementFactory, ElementResolver elementResolver, KeyEncoder keyEncoder) {
        this.elementFactory = elementFactory;
        this.elementResolver = elementResolver;
        this.keyEncoder = keyEncoder;
    }

    static OrientDbObjectConverter create(Function<MetaClass<?>, OElement> elementFactory, ElementResolver elementResolver, KeyEncoder keyEncoder) {
        return new OrientDbObjectConverter(elementFactory, elementResolver, keyEncoder);
    }

    static OrientDbObjectConverter create(KeyEncoder keyEncoder) {
        return new OrientDbObjectConverter(meta -> new ODocument(meta.simpleName()), (converter, hasMetaClass) -> null, keyEncoder);
    }

    @SuppressWarnings("unchecked")
    <S> Object toOrientDbObject(S entity) {
        if (entity instanceof Collection) {
            return convertCollection((Collection<?>)entity);
        } else if (entity instanceof Map) {
            return convertMap((Map<?, ?>)entity);
        } else if (!(entity instanceof HasMetaClass)) {
            return entity;
        }

        HasMetaClass<S> hasMetaClass = (HasMetaClass<S>)entity;
        MetaClass<S> metaClass = hasMetaClass.metaClass();
        OElement oElement = entity instanceof HasMetaClassWithKey ? elementFactory.apply(metaClass) : new ODocument();
        metaClass.properties().forEach(p -> {
            if (PropertyMetas.isReference(p) && p.getValue(entity) != null) {
                HasMetaClassWithKey<?, ?> referencedEntity = (HasMetaClassWithKey<?, ?>)p.getValue(entity);
                Object refElement = Optional
                        .ofNullable(elementResolver.resolve(this, referencedEntity))
                        .orElseGet(() -> (OElement)toOrientDbObject(referencedEntity));
                oElement.setProperty(p.name(), refElement);
            } else {
                Optional.ofNullable(toOrientDbObject(p.getValue(entity)))
                        .ifPresent(value -> oElement.setProperty(p.name(), value));
            }

            if (p.type().isSubtypeOf(HasMetaClass.class) && !p.type().isSubtypeOf(HasMetaClassWithKey.class)) {
                Optional.ofNullable(p.getValue(entity))
                        .map(keyEncoder::encode)
                        .ifPresent(str -> oElement.setProperty(p.name() + "AsString", str));
            }
        });

        return oElement;
    }

    private Map<?, ?> convertMap(Map<?, ?> map) {
        return map.entrySet()
                .stream()
                .collect(Collectors.toMap(e -> toOrientDbObject(e.getKey()), e -> toOrientDbObject(e.getValue())));
    }

    private Collection<?> convertCollection(Collection<?> collection) {
        if (collection instanceof List) {
            return collection.stream().map(this::toOrientDbObject).collect(Collectors.toList());
        } else if (collection instanceof Set) {
            return collection.stream().map(this::toOrientDbObject).collect(Collectors.toSet());
        }
        return collection;
    }
}
