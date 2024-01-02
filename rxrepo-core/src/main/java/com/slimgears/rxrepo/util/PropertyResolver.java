package com.slimgears.rxrepo.util;

import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;
import com.slimgears.util.autovalue.annotations.HasMetaClass;
import com.slimgears.util.autovalue.annotations.MetaClass;
import com.slimgears.util.autovalue.annotations.MetaClasses;
import com.slimgears.util.autovalue.annotations.PropertyMeta;
import com.slimgears.util.reflect.TypeTokens;

import java.util.Optional;

@SuppressWarnings("UnstableApiUsage")
public interface PropertyResolver {
    Iterable<String> propertyNames();
    Object getProperty(String name, Class<?> type);
    //Object getKey(Class<?> keyClass);

    @SuppressWarnings("unchecked")
    default <V> V getProperty(PropertyMeta<?, V> propertyMeta) {
        Object value = getProperty(propertyMeta.name(), propertyMeta.type().getRawType());
        return (value instanceof PropertyResolver)
                ? ((PropertyResolver)value).toObject(propertyMeta.type())
                : (V)value;
    }

    static PropertyResolver empty() {
        return PropertyResolvers.empty();
    }

    default <T> T toObject(MetaClass<T> metaClass) {
        return PropertyResolvers.toObject(this, metaClass);
    }

    default PropertyResolver mergeWith(PropertyResolver propertyResolver) {
        return PropertyResolvers.merge(propertyResolver, this);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    default <T> T toObject(TypeToken<T> typeToken) {
        if (typeToken.isSubtypeOf(HasMetaClass.class)) {
            return (T)toObject(MetaClasses.forToken((TypeToken)typeToken));
        } else {
            return Optional.ofNullable(propertyNames())
                    .map(names -> Iterables.getFirst(names, null))
                    .map(name -> (T)getProperty(name, Object.class))
                    .orElse(null);
        }
    }

    default PropertyResolver cache() {
        return CachedPropertyResolver.of(this);
    }

    static <T> PropertyResolver fromObject(MetaClass<T> metaClass, T obj) {
        return PropertyResolvers.fromObject(metaClass, obj);
    }
}
