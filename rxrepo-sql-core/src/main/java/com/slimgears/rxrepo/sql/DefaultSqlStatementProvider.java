package com.slimgears.rxrepo.sql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import com.slimgears.rxrepo.expressions.CollectionExpression;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.rxrepo.query.provider.*;
import com.slimgears.rxrepo.util.PropertyExpressionValueProvider;
import com.slimgears.rxrepo.util.PropertyExpressions;
import com.slimgears.rxrepo.util.PropertyMetas;
import com.slimgears.rxrepo.util.PropertyResolver;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClasses;
import com.slimgears.util.autovalue.annotations.PropertyMeta;
import com.slimgears.util.stream.Lazy;
import com.slimgears.util.stream.Optionals;
import com.slimgears.util.stream.Streams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.slimgears.rxrepo.sql.SqlStatement.of;
import static com.slimgears.rxrepo.sql.StatementUtils.concat;

@SuppressWarnings("UnstableApiUsage")
public class DefaultSqlStatementProvider implements SqlStatementProvider {
    protected final static Logger log = LoggerFactory.getLogger(DefaultSqlStatementProvider.class);

    protected interface Assignment {
        String name();
        String value();

        static Assignment of(String name, String value) {
            return new Assignment() {
                @Override
                public String name() {
                    return name;
                }

                @Override
                public String value() {
                    return value;
                }
            };
        }
    }

    protected final SqlExpressionGenerator sqlExpressionGenerator;
    private final Supplier<String> dbNameSupplier;
    private final SqlTypeMapper sqlTypeMapper;

    public DefaultSqlStatementProvider(SqlExpressionGenerator sqlExpressionGenerator,
                                       SqlTypeMapper sqlTypeMapper,
                                       Supplier<String> dbNameSupplier) {
        this.sqlExpressionGenerator = sqlExpressionGenerator;
        this.dbNameSupplier = dbNameSupplier;
        this.sqlTypeMapper = sqlTypeMapper;
    }

    @Override
    public <K, S, T> SqlStatement forQuery(QueryInfo<K, S, T> queryInfo) {
        return statement(() -> of(
                selectClause(queryInfo),
                fromClause(queryInfo),
                whereClause(queryInfo),
                orderClause(queryInfo),
                limitClause(queryInfo),
                skipClause(queryInfo)));
    }

    @Override
    public <K, S, T, R> SqlStatement forAggregation(QueryInfo<K, S, T> queryInfo, ObjectExpression<T, R> aggregation, String projectedName) {
        return statement(() -> of(
                selectClause(queryInfo, aggregation, projectedName),
                fromClause(queryInfo),
                whereClause(queryInfo)));
    }

    @Override
    public <K, S> SqlStatement forUpdate(UpdateInfo<K, S> updateInfo) {
        return statement(() -> forUpdateStatement(updateInfo));
    }

    @Override
    public <K, S> SqlStatement forDelete(DeleteInfo<K, S> deleteInfo) {
        return statement(() -> of(
                "delete",
                fromClause(deleteInfo),
                whereClause(deleteInfo),
                limitClause(deleteInfo)));
    }

    @Override
    public <K, S> SqlStatement forUpdate(MetaClassWithKey<K, S> metaClass, PropertyResolver propertyResolver, SqlReferenceResolver resolver) {
        return forInsertOrUpdate(metaClass, propertyResolver, resolver, false);
    }

    @Override
    public <K, S> SqlStatement forInsertOrUpdate(MetaClassWithKey<K, S> metaClass, PropertyResolver propertyResolver, SqlReferenceResolver resolver) {
        return forInsertOrUpdate(metaClass, propertyResolver, resolver, true);
    }

    @Override
    public <K, S> SqlStatement forInsert(MetaClassWithKey<K, S> metaClass, Iterable<PropertyResolver> propertyResolvers, SqlReferenceResolver referenceResolver) {
        return statement(() -> forBatchInsertStatement(metaClass, propertyResolvers, referenceResolver));
    }

    @SuppressWarnings("unchecked")
    protected <K, S> SqlStatement forInsertOrUpdate(MetaClassWithKey<K, S> metaClass, PropertyResolver propertyResolver, SqlReferenceResolver resolver, boolean forced) {
        return statement(() -> {
            SqlStatement insertStatement = forInsertStatement(metaClass, propertyResolver, resolver);
            Lazy<S> object = Lazy.of(() -> propertyResolver.toObject(metaClass));
            UpdateInfo<K, S> updateInfo = UpdateInfo.<K, S>builder()
                    .metaClass(metaClass)
                    .propertyUpdates(PropertyExpressions.valueProvidersFromMeta(metaClass)
                            .flatMap(p -> Optional.ofNullable(p.value(object.get()))
                                    .map(v -> PropertyUpdateInfo.create((PropertyExpression<S, ?, Object>)p.property(), v))
                                    .map(Stream::of)
                                    .orElseGet(Stream::empty))
                            .collect(ImmutableList.toImmutableList()))
                    .build();
            SqlStatement updateStatement = forUpdateStatement(updateInfo);
            return of(insertStatement.statement() + "\n" +
                    "on conflict(" + metaClass.keyProperty().name() + ") do\n" +
                    updateStatement.statement().replace("update " + fullTableName(metaClass), "update "));
        });
    }

    @Override
    public <K, S> SqlStatement forDropTable(MetaClassWithKey<K, S> metaClass) {
        return statement(() -> of("drop table", fullTableName(metaClass), "cascade"));
    }

    @Override
    public <K, S> SqlStatement forCreateTable(MetaClassWithKey<K, S> metaClass) {
        return statement(() -> of("create table if not exists",
                fullTableName(metaClass),
                fieldDefs(metaClass).collect(Collectors.joining(",\n", "(\n", ")"))));
    }

    @Override
    public SqlStatement forDropSchema() {
        return statement(() -> of("drop schema", databaseName(), "cascade"));
    }

    @Override
    public <K, S> SqlStatement forInsert(MetaClassWithKey<K, S> metaClass, PropertyResolver propertyResolver, SqlReferenceResolver resolver) {
        return statement(() -> forInsertStatement(metaClass, propertyResolver, resolver));
    }

    protected <K, S> Stream<String> fieldDefs(MetaClassWithKey<K, S> metaClass) {
        return PropertyExpressions.embeddedPropertiesForMeta(metaClass)
                .map(this::toFieldDef);
    }

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
                limitClause(updateInfo));
    }

    protected <K, S> SqlStatement forInsertStatement(MetaClassWithKey<K, S> metaClass, PropertyResolver propertyResolver, SqlReferenceResolver resolver) {
        Collection<Assignment> assignments = toAssignments(metaClass, propertyResolver, resolver)
                .collect(Collectors.toList());

        return of(
                "insert",
                "into",
                fullTableName(metaClass),
                assignments.stream().map(Assignment::name)
                        .map(this::fullFieldName)
                        .collect(Collectors.joining(", ", "(", ")")),
                "values",
                assignments.stream().map(Assignment::value).collect(Collectors.joining(", ", "(", ")"))
        );
    }

    protected <K, S> SqlStatement forBatchInsertStatement(MetaClassWithKey<K, S> metaClass, Iterable<PropertyResolver> propertyResolvers, SqlReferenceResolver resolver) {
        Set<String> fields = Sets.newLinkedHashSet();
        List<Map<String, String>> values = Streams.fromIterable(propertyResolvers)
                .map(pr -> toAssignments(metaClass, pr, resolver).collect(ImmutableMap.toImmutableMap(Assignment::name, Assignment::value)))
                .peek(map -> fields.addAll(map.keySet()))
                .collect(Collectors.toList());

        return of(
                "insert",
                "into",
                fullTableName(metaClass),
                fields.stream().map(this::fullFieldName).collect(Collectors.joining(", ", "(", ")")),
                "values",
                values
                        .stream()
                        .map(valMap -> fields.stream().map(field -> Optional.ofNullable(valMap.get(field)).orElse("null")).collect(Collectors.joining(", ", "(", ")")))
                        .collect(Collectors.joining("\n,"))
        );
    }

    @Override
    public <K, S> String tableName(MetaClassWithKey<K, S> metaClass) {
        return metaClass.simpleName();
    }

    @Override
    public String databaseName() {
        return dbNameSupplier.get();
    }

    @Override
    public SqlStatement forCreateSchema() {
        return of("create schema if not exists", databaseName());
    }

    protected SqlStatement statement(Supplier<SqlStatement> statementSupplier) {
        List<Object> params = new ArrayList<>();
        SqlStatement statement = sqlExpressionGenerator.withParams(params, statementSupplier::get);
        return statement.withArgs(params.toArray());
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private <K, S, T, Q extends HasMapping<S, T> & HasEntityMeta<K, S> & HasProperties<T>> String selectClause(Q queryInfo) {
        ObjectExpression<S, T> expression = Optional
                .ofNullable(queryInfo.mapping())
                .orElse(ObjectExpression.arg((TypeToken)queryInfo.metaClass().asType()));

        String selectOperator = Optional.ofNullable(queryInfo.distinct()).orElse(false) ? "select distinct" : "select";

        return Optional.of(toMappingClause(queryInfo.metaClass(), expression, queryInfo.properties()))
                .filter(exp -> !exp.isEmpty())
                .map(exp -> concat(selectOperator, exp))
                .orElse(selectOperator);
    }

    protected <K, S, T, R, Q extends HasMapping<S, T> & HasEntityMeta<K, S> & HasProperties<T>> String selectClause(Q statement, ObjectExpression<T, R> aggregation, String projectedName) {
        return concat(
                "select",
                Optional.ofNullable(statement.mapping())
                        .map(exp -> sqlExpressionGenerator.toSqlExpression(aggregation, exp))
                        .orElseGet(() -> sqlExpressionGenerator.toSqlExpression(aggregation, "*")),
                "as",
                projectedName);
    }

    protected <S> String toFieldDef(PropertyExpression<S, ?, ?> propertyExpression) {
        String type = toSqlType(toFieldType(propertyExpression.property()));
        return Stream.concat(
                Stream.of(fullFieldName(propertyExpression), type),
                toFieldConstraints(propertyExpression))
                .collect(Collectors.joining(" "));
    }

    protected String toSqlType(TypeToken<?> type) {
        return sqlTypeMapper.toSqlType(type);
    }

    protected String toSqlType(Class<?> type) {
        return toSqlType(TypeToken.of(type));
    }

    protected Stream<String> toFieldConstraints(PropertyExpression<?, ?, ?> propertyExpression) {
        ImmutableList.Builder<String> builder = ImmutableList.builder();
        if (PropertyMetas.isReference(propertyExpression.property())) {
            builder.add(toForeignKeyDef(propertyExpression.property()));
        } else if (PropertyMetas.isKey(propertyExpression.property())) {
            builder.add("primary key");
        }

        if (PropertyExpressions.isMandatory(propertyExpression)) {
            builder.add("not null");
        }

        return builder.build().stream();
    }

    protected String toForeignKeyDef(PropertyMeta<?, ?> referenceProperty) {
        MetaClassWithKey<?, ?> metaClassWithKey = MetaClasses.forTokenWithKeyUnchecked(referenceProperty.type());
        StringBuilder builder = new StringBuilder();
        builder
                .append("references ").append(fullTableName(metaClassWithKey))
                .append("(").append(metaClassWithKey.keyProperty().name()).append(")");

        if (!PropertyMetas.isMandatory(referenceProperty)) {
            builder.append(" on delete set null");
        }

        return builder.toString();
    }

    private TypeToken<?> toFieldType(PropertyMeta<?, ?> property) {
        if (PropertyMetas.isEmbedded(property)) {
            return TypeToken.of(String.class);
        } else if (PropertyMetas.isReference(property)) {
            MetaClassWithKey<?, ?> refMeta = PropertyMetas.getReferencedType(property)
                    .map(MetaClasses::forTokenWithKeyUnchecked)
                    .orElseThrow(() -> new RuntimeException("Could not determine referenced meta type"));
            return toFieldType(refMeta.keyProperty());
        }
        return property.type();
    }

    protected <S, T> String toMappingClause(MetaClassWithKey<?, S> metaClass, ObjectExpression<S, T> expression, Collection<PropertyExpression<T, ?, ?>> properties) {
        return Optionals.or(
                () -> Optional.ofNullable(properties)
                        .map(this::eliminateRedundantProperties)
                        .filter(p -> !p.isEmpty())
                        .map(p -> toProjectionFields(metaClass, expression, p).collect(Collectors.joining(", ")))
                        .filter(e -> !e.isEmpty()),
                () -> Optional
                        .of(sqlExpressionGenerator.toSqlExpression(expression))
                        .filter(e -> !e.isEmpty()))
                .orElseGet(() -> toProjectionFields(
                        metaClass,
                        expression,
                        eliminateRedundantProperties(PropertyExpressions.mandatoryProperties(expression.reflect().objectType()).collect(Collectors.toList())))
                        .collect(Collectors.joining(", ")));
    }

    protected <S, T> Stream<String> toProjectionFields(MetaClassWithKey<?, S> metaClass, ObjectExpression<S, T> expression, Collection<PropertyExpression<T, ?, ?>> properties) {
        return properties.stream()
                .map(prop -> sqlExpressionGenerator.toSqlExpression(prop, expression));
    }

    private <K, S, Q extends HasEntityMeta<K, S>> String fromClause(Q statement) {
        return "from " + fullTableName(statement.metaClass());
    }

    protected <S, Q extends HasPredicate<S> & HasEntityMeta<?, S>> String whereClauseForUpdate(Q statement) {
        return whereClause(statement);
    }

    protected <S, Q extends HasPredicate<S>> String whereClause(Q statement) {
        return Optional
                .ofNullable(statement.predicate())
                .map(this::toConditionClause)
                .map(cond -> "where " + cond)
                .orElse("");
    }

    protected <K, T> Stream<Assignment> toAssignments(MetaClassWithKey<K, T> metaClass,
                                                      PropertyResolver propertyResolver,
                                                      SqlReferenceResolver referenceResolver) {
        T object = propertyResolver.toObject(metaClass);
        return toAssignableProperties(metaClass)
                .map(PropertyExpressionValueProvider::fromProperty)
                .flatMap(vp -> toAssignment(vp, object, referenceResolver))
                .filter(entry -> entry.value() != null);
    }

    protected <K, T> Stream<PropertyExpression<T, ?, ?>> toAssignableProperties(MetaClassWithKey<K, T> metaClass) {
        return PropertyExpressions.embeddedPropertiesForMeta(metaClass);
    }

    protected <T> Stream<Assignment> toAssignment(PropertyExpressionValueProvider<T, ?, ?> vp, T object, SqlReferenceResolver referenceResolver) {
        return Stream.of(Assignment.of(sqlExpressionGenerator.toSqlExpression(vp.property()),
                toValue(vp.value(object), referenceResolver)));
    }

    protected String toValue(Object value, SqlReferenceResolver referenceResolver) {
        if (value == null) {
            return null;
        }

        String strValue;

        if (value instanceof HasMetaClassWithKey) {
            HasMetaClassWithKey<?, ?> hasMetaClassWithKey = (HasMetaClassWithKey<?, ?>)value;
            strValue = sqlExpressionGenerator.fromStatement(reference(hasMetaClassWithKey, referenceResolver));
        } else {
            strValue = sqlExpressionGenerator.fromConstant(value);
        }

        return strValue;
    }

    @SuppressWarnings("unchecked")
    private <K, S> SqlStatement reference(HasMetaClassWithKey<K, S> value, SqlReferenceResolver referenceResolver) {
        return referenceResolver.toReferenceValue(value.metaClass(), value.metaClass().keyOf((S)value));
    }

    protected <Q extends HasLimit> String limitClause(Q statement) {
        return Optional.ofNullable(statement.limit())
                .map(count -> "limit " + count)
                .orElse("");
    }

    private <Q extends HasPagination> String skipClause(Q statement) {
        return Optional.ofNullable(statement.skip())
                .map(count -> "skip " + count)
                .orElse("");
    }

    private <T, Q extends HasSortingInfo<T>> String orderClause(Q statement) {
        if (statement.sorting().isEmpty()) {
            return "";
        }

        StringBuilder builder = new StringBuilder();
        SortingInfo<T, ?, ?> first = statement.sorting().get(0);
        builder.append("order by ");
        builder.append(toOrder(first));
        statement.sorting().stream().skip(1)
                .forEach(si -> {
                    builder.append(", ");
                    builder.append(toOrder(si));
                });
        return builder.toString();
    }

    private String toOrder(SortingInfo<?, ?, ?> sortingInfo) {
        return sqlExpressionGenerator.toSqlExpression(sortingInfo.property()) + (sortingInfo.ascending() ? " asc" : " desc");
    }

    protected <S> String toConditionClause(ObjectExpression<S, Boolean> condition) {
        return sqlExpressionGenerator.toSqlExpression(condition);
    }

    protected  <T> Collection<PropertyExpression<T, ?, ?>> eliminateRedundantProperties(Collection<PropertyExpression<T, ?, ?>> properties) {
        Set<PropertyExpression<T, ?, ?>> propertySet = Sets.newLinkedHashSet(properties);

        properties.stream()
                .flatMap(PropertyExpressions::parentProperties)
                .forEach(propertySet::remove);

        return propertySet;
    }

    protected String fullTableName(MetaClassWithKey<?, ?> metaClass) {
        return databaseName() + "." + tableName(metaClass);
    }

    protected String fullFieldName(PropertyExpression<?, ?, ?> propertyExpression) {
        return fullFieldName(sqlExpressionGenerator.toSqlExpression(propertyExpression));
    }

    protected String fullFieldName(String fieldName) {
        return fieldName;
    }
}
