package com.slimgears.rxrepo.util;

import com.slimgears.rxrepo.annotations.Embedded;
import com.slimgears.util.autovalue.annotations.HasMetaClass;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import com.slimgears.util.autovalue.annotations.PropertyMeta;
import com.slimgears.util.reflect.TypeToken;
import com.slimgears.util.stream.Optionals;

import java.util.Optional;

@SuppressWarnings("WeakerAccess")
public class PropertyMetas {
    public static boolean isReference(PropertyMeta<?, ?> propertyMeta) {
        return isReference(propertyMeta.type()) && !propertyMeta.hasAnnotation(Embedded.class);
    }

    public static boolean isEmbedded(PropertyMeta<?, ?> propertyMeta) {
        return isEmbedded(propertyMeta.type()) || (isReference(propertyMeta.type()) && propertyMeta.hasAnnotation(Embedded.class));
    }

    public static boolean isReference(TypeToken<?> typeToken) {
        return typeToken.is(HasMetaClassWithKey.class::isAssignableFrom);
    }

    public static boolean isEmbedded(TypeToken<?> typeToken) {
        return hasMetaClass(typeToken) && !isReference(typeToken);
    }

    public static boolean hasMetaClass(TypeToken<?> typeToken) {
        return typeToken.is(HasMetaClass.class::isAssignableFrom);
    }

    public static boolean hasMetaClass(PropertyMeta<?, ?> property) {
        return hasMetaClass(property.type());
    }

    public static boolean isKey(PropertyMeta<?, ?> property) {
        return Optional.of(property.declaringType())
                .flatMap(Optionals.ofType(MetaClassWithKey.class))
                .map(mc -> mc.keyProperty() == property)
                .orElse(false);
    }
}
