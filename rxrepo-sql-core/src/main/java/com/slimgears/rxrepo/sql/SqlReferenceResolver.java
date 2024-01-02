package com.slimgears.rxrepo.sql;

import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;

public interface SqlReferenceResolver {
    <K, S> SqlStatement toReferenceValue(MetaClassWithKey<K, S> metaClass, K key);

    default <K, S extends HasMetaClassWithKey<K, S>> SqlStatement toReferenceValue(S referencedEntity) {
        return toReferenceValue(referencedEntity.metaClass(), referencedEntity.metaClass().keyProperty().getValue(referencedEntity));
    }
}
