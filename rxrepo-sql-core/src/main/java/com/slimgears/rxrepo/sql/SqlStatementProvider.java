package com.slimgears.rxrepo.sql;

import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.query.provider.DeleteInfo;
import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.rxrepo.query.provider.UpdateInfo;
import com.slimgears.rxrepo.util.PropertyResolver;
import com.slimgears.util.autovalue.annotations.MetaClass;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;

public interface SqlStatementProvider {
    <K, S, T> SqlStatement forQuery(QueryInfo<K, S, T> queryInfo);

    <K, S, T, R> SqlStatement forAggregation(QueryInfo<K, S, T> queryInfo,
                                             ObjectExpression<T, R> aggregation,
                                             String projectedName);

    <K, S> SqlStatement forUpdate(UpdateInfo<K, S> updateInfo);

    <K, S> SqlStatement forDelete(DeleteInfo<K, S> deleteInfo);

    <K, S> SqlStatement forInsert(MetaClassWithKey<K, S> metaClass,
                                  Iterable<PropertyResolver> propertyResolvers,
                                  SqlReferenceResolver referenceResolver);

    <K, S> SqlStatement forInsert(MetaClassWithKey<K, S> metaClass,
                                  PropertyResolver propertyResolver,
                                  SqlReferenceResolver referenceResolver);

    <K, S> SqlStatement forInsertOrUpdate(MetaClassWithKey<K, S> metaClass,
                                          PropertyResolver propertyResolver,
                                          SqlReferenceResolver referenceResolver);

    <K, S> SqlStatement forUpdate(MetaClassWithKey<K, S> metaClass,
                                  PropertyResolver propertyResolver,
                                  SqlReferenceResolver referenceResolver);

    <K, S> SqlStatement forDropTable(MetaClassWithKey<K, S> metaClass);

    <K, S> SqlStatement forCreateTable(MetaClassWithKey<K, S> metaClass);

    <K, S> String tableName(MetaClassWithKey<K, S> metaClass);

    String databaseName();

    SqlStatement forCreateSchema();
    SqlStatement forDropSchema();

    default <K, S> SqlStatement forInsertOrUpdate(MetaClassWithKey<K, S> metaClass, S entity, SqlReferenceResolver referenceResolver) {
        return forInsertOrUpdate(metaClass, PropertyResolver.fromObject(metaClass, entity), referenceResolver);
    }

    default <K, S> SqlStatement forInsert(MetaClassWithKey<K, S> metaClass, S entity, SqlReferenceResolver referenceResolver) {
        return forInsert(metaClass, PropertyResolver.fromObject(metaClass, entity), referenceResolver);
    }
}
