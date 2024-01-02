package com.slimgears.rxrepo.postgres;

import com.google.common.base.Strings;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.rxrepo.query.provider.HasEntityMeta;
import com.slimgears.rxrepo.query.provider.HasPredicate;
import com.slimgears.rxrepo.query.provider.UpdateInfo;
import com.slimgears.rxrepo.sql.*;
import com.slimgears.rxrepo.util.PropertyResolver;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import com.slimgears.util.generic.MoreStrings;

import java.util.Collection;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.slimgears.rxrepo.sql.SqlStatement.of;
import static com.slimgears.rxrepo.sql.StatementUtils.concat;

public class PostgresSqlStatementProvider extends DefaultSqlStatementProvider {
    private static final String sequenceName = "generation";

    public PostgresSqlStatementProvider(SqlExpressionGenerator sqlExpressionGenerator,
                                        SqlTypeMapper sqlTypeMapper,
                                        Supplier<String> dbNameSupplier) {
        super(sqlExpressionGenerator, sqlTypeMapper, dbNameSupplier);
    }

    @Override
    public SqlStatement forCreateSchema() {
         return super.forCreateSchema().append(SqlStatement.of("create sequence if not exists", sequenceName()));
    }

    @Override
    protected <K, S> Stream<String> fieldDefs(MetaClassWithKey<K, S> metaClass) {
        return Stream.concat(
                super.fieldDefs(metaClass),
                Stream.of("\"" + SqlFields.sequenceFieldName + "\" " + toSqlType(Long.class)));
    }

    @Override
    protected <S, T> Stream<String> toProjectionFields(MetaClassWithKey<?, S> metaClass, ObjectExpression<S, T> expression, Collection<PropertyExpression<T, ?, ?>> properties) {
        return Stream.concat(
                super.toProjectionFields(metaClass, expression, properties),
                Stream.of("\"" + SqlFields.sequenceFieldName + "\""));
    }

    @Override
    protected <S, Q extends HasPredicate<S> & HasEntityMeta<?, S>> String whereClauseForUpdate(Q statement) {
        return Optional.of(super.whereClauseForUpdate(statement))
                .map(Strings::emptyToNull)
                .map(w -> MoreStrings.format("{} AND ({}.\"{}\" <= currval('{}'))", w, fullTableName(statement.metaClass()), SqlFields.sequenceFieldName, sequenceName()))
                .orElseGet(() -> MoreStrings.format("where {}.\"{}\" <= currval('{}')", fullTableName(statement.metaClass()), SqlFields.sequenceFieldName, sequenceName()));
    }

    @Override
    protected <K, T> Stream<Assignment> toAssignments(MetaClassWithKey<K, T> metaClass, PropertyResolver propertyResolver, SqlReferenceResolver referenceResolver) {
        return Stream.concat(
                super.toAssignments(metaClass, propertyResolver, referenceResolver),
                Stream.of(Assignment.of("\"" + SqlFields.sequenceFieldName + "\"", "nextval('" + sequenceName() + "')")));
    }

    @Override
    protected <K, S> SqlStatement forUpdateStatement(UpdateInfo<K, S> updateInfo) {
        return of(
                "update",
                fullTableName(updateInfo.metaClass()),
                "set",
                updateInfo.propertyUpdates()
                        .stream()
                        .map(pu -> concat(sqlExpressionGenerator.toSqlExpression(pu.property()), "=", sqlExpressionGenerator.toSqlExpression(pu.updater())))
                        .collect(Collectors.joining(", ")),
                whereClauseForUpdate(updateInfo),
                "returning *");
    }


    private String sequenceName() {
        return databaseName() + "." + sequenceName;
    }
}
