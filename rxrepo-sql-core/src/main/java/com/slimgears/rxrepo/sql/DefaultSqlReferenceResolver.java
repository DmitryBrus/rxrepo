package com.slimgears.rxrepo.sql;

import com.slimgears.rxrepo.util.PropertyMetas;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;

public class DefaultSqlReferenceResolver implements SqlReferenceResolver {
    private final KeyEncoder keyEncoder;
    private final SqlExpressionGenerator expressionGenerator;

    public DefaultSqlReferenceResolver(KeyEncoder keyEncoder, SqlExpressionGenerator expressionGenerator) {
        this.keyEncoder = keyEncoder;
        this.expressionGenerator = expressionGenerator;
    }

    @Override
    public <K, S> SqlStatement toReferenceValue(MetaClassWithKey<K, S> metaClass, K key) {
        if (PropertyMetas.isEmbedded(metaClass.keyProperty())) {
            return SqlStatement.of(expressionGenerator.fromConstant(keyEncoder.encode(key)));
        }
        return SqlStatement.of(expressionGenerator.fromConstant(key));
    }
}
