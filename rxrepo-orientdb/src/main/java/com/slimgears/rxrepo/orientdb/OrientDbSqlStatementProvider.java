package com.slimgears.rxrepo.orientdb;

import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.expressions.PropertyExpression;
import com.slimgears.rxrepo.query.provider.*;
import com.slimgears.rxrepo.sql.*;
import com.slimgears.rxrepo.util.PropertyExpressionValueProvider;
import com.slimgears.rxrepo.util.PropertyExpressions;
import com.slimgears.rxrepo.util.PropertyMetas;
import com.slimgears.rxrepo.util.PropertyResolver;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import com.slimgears.util.autovalue.annotations.PropertyMeta;
import com.slimgears.util.generic.MoreStrings;
import com.slimgears.util.stream.Optionals;
import com.slimgears.util.stream.Streams;

import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.slimgears.rxrepo.sql.SqlStatement.of;
import static com.slimgears.rxrepo.sql.StatementUtils.concat;
import static com.slimgears.util.generic.LazyString.lazy;

@SuppressWarnings("UnstableApiUsage")
public class OrientDbSqlStatementProvider extends DefaultSqlStatementProvider {
    private final KeyEncoder keyEncoder;

    public OrientDbSqlStatementProvider(SqlExpressionGenerator sqlExpressionGenerator,
                                        SqlTypeMapper typeMapper,
                                        KeyEncoder keyEncoder,
                                        Supplier<String> dbNameSupplier) {
        super(sqlExpressionGenerator, typeMapper, dbNameSupplier);
        this.keyEncoder = keyEncoder;
    }

    @Override
    protected <K, S> SqlStatement forInsertOrUpdate(MetaClassWithKey<K, S> metaClass, PropertyResolver propertyResolver, SqlReferenceResolver resolver, boolean forced) {
        PropertyMeta<S, K> keyProperty = metaClass.keyProperty();

        return statement(() -> {
            Collection<Assignment> assignments = toAssignments(metaClass, propertyResolver, resolver)
                    .collect(Collectors.toList());
            return of(
                    "update",
                    tableName(metaClass),
                    "set",
                    Streams
                            .fromIterable(assignments)
                            .map(entry -> MoreStrings.format("{} = {}", entry.name(), entry.value()))
                            .collect(Collectors.joining(", ")),
                    forced ? "upsert" : "",
                    "return after",
                    "where",
                    toConditionClause(PropertyExpression.ofObject(keyProperty).eq(propertyResolver.getProperty(keyProperty)))
            );
        });
    }



    @Override
    protected <K, S> SqlStatement forInsertStatement(MetaClassWithKey<K, S> metaClass, PropertyResolver propertyResolver, SqlReferenceResolver resolver) {
        Collection<Assignment> assignments = toAssignments(metaClass, propertyResolver, resolver)
                .collect(Collectors.toList());

        return statement(() -> of(
                "insert",
                "into",
                fullTableName(metaClass),
                "set",
                assignments
                        .stream()
                        .map(a -> concat(fullFieldName(a.name()), "=", a.value()))
                        .collect(Collectors.joining(", ")),
                "return null"
        ));
    }

    @Override
    protected <S, T> Stream<String> toProjectionFields(MetaClassWithKey<?, S> metaClass, ObjectExpression<S, T> expression, Collection<PropertyExpression<T, ?, ?>> properties) {
        return Stream.concat(
                super.toProjectionFields(metaClass, expression, properties),
                Stream.of("`" + SqlFields.sequenceFieldName + "`"));
    }

    @Override
    protected String fullTableName(MetaClassWithKey<?, ?> metaClass) {
        return metaClass.simpleName();
    }

    @Override
    protected <K, T> Stream<PropertyExpression<T, ?, ?>> toAssignableProperties(MetaClassWithKey<K, T> metaClass) {
        return PropertyExpressions.ownPropertiesOf(metaClass.asType()).map(p -> p);
    }

    @Override
    protected <K, T> Stream<Assignment> toAssignments(MetaClassWithKey<K, T> metaClass, PropertyResolver propertyResolver, SqlReferenceResolver referenceResolver) {
        return Stream.of(
                super.toAssignments(metaClass, propertyResolver, referenceResolver),
                Stream.of(propertyResolver.getProperty("@version", Integer.class))
                        .filter(Objects::nonNull)
                        .map(ver -> Assignment.of("`@version`", sqlExpressionGenerator.fromConstant(ver))),
                Stream.of(Assignment.of("`" + SqlFields.sequenceFieldName + "`", MoreStrings.format("sequence('{}').next()", OrientDbSqlSchemaGenerator.sequenceName))))
                .flatMap(Function.identity());
    }

    @Override
    protected <T> Stream<Assignment> toAssignment(PropertyExpressionValueProvider<T, ?, ?> vp, T object, SqlReferenceResolver referenceResolver) {
        if (PropertyMetas.isEmbedded(vp.property().property())) {
            String name = sqlExpressionGenerator.toSqlExpression(vp.property()) + "`AsString`";
            String assignmentName = name.replaceAll("``", "");
//            return Stream.of(Assignment.of(name, sqlExpressionGenerator.fromConstant(keyEncoder.encode(vp.value(object)))));
            return Stream.concat(
                    Optional.ofNullable(vp.value(object))
                            .map(keyEncoder::encode)
                            .map(sqlExpressionGenerator::fromConstant)
                            .map(val -> Assignment.of(assignmentName, val))
                            .map(Stream::of)
                            .orElseGet(Stream::empty),
                    super.toAssignment(vp, object, referenceResolver));
        } else {
            return super.toAssignment(vp, object, referenceResolver);
        }
    }

    @Override
    protected <S, T> String toMappingClause(MetaClassWithKey<?, S> metaClass, ObjectExpression<S, T> expression, Collection<PropertyExpression<T, ?, ?>> properties) {
        return Optionals.or(
                () -> Optional.ofNullable(properties)
                        .filter(p -> !p.isEmpty())
                        .map(this::eliminateRedundantProperties)
                        .map(p -> Stream.concat(
                                p.stream().map(prop -> sqlExpressionGenerator.toSqlExpression(prop, expression)),
                                Stream.of("`" + SqlFields.sequenceFieldName + "`"))
                                .collect(Collectors.joining(", "))),
                () -> Optional
                        .of(sqlExpressionGenerator.toSqlExpression(expression))
                        .filter(e -> !e.isEmpty()))
                .orElse("");
    }






















//
//
//    @Override
//    public <K, S, T> SqlStatement forQuery(QueryInfo<K, S, T> queryInfo) {
//        return statement(() -> of(
//                selectClause(queryInfo),
//                fromClause(queryInfo),
//                whereClause(queryInfo),
//                orderClause(queryInfo),
//                limitClause(queryInfo),
//                skipClause(queryInfo)));
//    }
//
//    @Override
//    public <K, S, T, R> SqlStatement forAggregation(QueryInfo<K, S, T> queryInfo, ObjectExpression<T, R> aggregation, String projectedName) {
//        return statement(() -> of(
//                selectClause(queryInfo, aggregation, projectedName),
//                fromClause(queryInfo),
//                whereClause(queryInfo)));
//    }
//
//
//    @Override
//    public <K, S> SqlStatement forDelete(DeleteInfo<K, S> deleteInfo) {
//        return statement(() -> of(
//                "delete",
//                fromClause(deleteInfo),
//                whereClause(deleteInfo),
//                limitClause(deleteInfo)));
//    }
//
//    @Override
//    public <K, S> SqlStatement forUpdate(MetaClassWithKey<K, S> metaClass, PropertyResolver propertyResolver, SqlReferenceResolver resolver) {
//        return forInsertOrUpdate(metaClass, propertyResolver, resolver, false);
//    }
//
//    @Override
//    public <K, S> SqlStatement forInsertOrUpdate(MetaClassWithKey<K, S> metaClass, PropertyResolver propertyResolver, SqlReferenceResolver resolver) {
//        return forInsertOrUpdate(metaClass, propertyResolver, resolver, true);
//    }
//
////    @Override
////    protected <K, S> SqlStatement forInsertOrUpdate(MetaClassWithKey<K, S> metaClass, PropertyResolver propertyResolver, SqlReferenceResolver resolver, boolean forced) {
////        PropertyMeta<S, K> keyProperty = metaClass.keyProperty();
////
////        return statement(() -> of(
////                "update",
////                tableName(metaClass),
////                "set",
////                Streams
////                        .fromIterable(propertyResolver.propertyNames())
////                        .flatMap(sqlAssignmentGenerator.toAssignment(metaClass, propertyResolver, resolver))
////                        .collect(Collectors.joining(", ")),
////                forced ? "upsert" : "",
////                "return after",
////                "where",
////                toConditionClause(PropertyExpression.ofObject(keyProperty).eq(propertyResolver.getProperty(keyProperty)))
////        ));
////    }
//
//    @Override
//    public <K, S> SqlStatement forDropTable(MetaClassWithKey<K, S> metaClass) {
//        return statement(() -> of("drop", "table", tableName(metaClass)));
//    }
//
//    @Override
//    public SqlStatement forDropSchema() {
//        return statement(() -> of("drop", "database", databaseName()));
//    }
//
//    @Override
//    public <K, S> SqlStatement forInsert(MetaClassWithKey<K, S> metaClass, PropertyResolver propertyResolver, SqlReferenceResolver resolver) {
//        return statement(() -> of(
//                "insert",
//                "into",
//                tableName(metaClass),
//                "set",
//                Streams
//                        .fromIterable(propertyResolver.propertyNames())
//                        .flatMap(sqlAssignmentGenerator.toAssignment(metaClass, propertyResolver, resolver))
//                        .collect(Collectors.joining(", "))
//        ));
//    }
//
//    @Override
//    protected SqlStatement statement(Supplier<SqlStatement> statementSupplier) {
//        List<Object> params = new ArrayList<>();
//        SqlStatement statement = sqlExpressionGenerator.withParams(params, statementSupplier::get);
//        return statement.withArgs(params.toArray());
//    }
//
//    @SuppressWarnings({"unchecked", "rawtypes"})
//    private <K, S, T, Q extends HasMapping<S, T> & HasEntityMeta<K, S> & HasProperties<T>> String selectClause(Q queryInfo) {
//        ObjectExpression<S, T> expression = Optional
//                .ofNullable(queryInfo.mapping())
//                .orElse(ObjectExpression.arg((TypeToken)queryInfo.metaClass().asType()));
//
//        String selectOperator = Optional.ofNullable(queryInfo.distinct()).orElse(false) ? "select distinct" : "select";
//
//        return Optional.of(toMappingClause(expression, queryInfo.properties()))
//                .filter(exp -> !exp.isEmpty())
//                .map(exp -> concat(selectOperator, exp))
//                .orElse(selectOperator);
//    }
//
//    @Override
//    protected  <K, S, T, R, Q extends HasMapping<S, T> & HasEntityMeta<K, S> & HasProperties<T>> String selectClause(Q statement, ObjectExpression<T, R> aggregation, String projectedName) {
//        return concat(
//                "select",
//                Optional.ofNullable(statement.mapping())
//                        .map(exp -> sqlExpressionGenerator.toSqlExpression(aggregation, exp))
//                        .orElseGet(() -> sqlExpressionGenerator.toSqlExpression(aggregation)),
//                "as",
//                projectedName);
//    }
//
//    private <S, T> String toMappingClause(ObjectExpression<S, T> expression, Collection<PropertyExpression<T, ?, ?>> properties) {
//        return Optionals.or(
//                () -> Optional.ofNullable(properties)
//                        .filter(p -> !p.isEmpty())
//                        .map(this::eliminateRedundantProperties)
//                        .map(p -> Stream.concat(
//                                p.stream().map(prop -> sqlExpressionGenerator.toSqlExpression(prop, expression)),
//                                Stream.of("`" + SqlFields.sequenceFieldName + "`"))
//                                .collect(Collectors.joining(", "))),
//                () -> Optional
//                        .of(sqlExpressionGenerator.toSqlExpression(expression))
//                        .filter(e -> !e.isEmpty()))
//                .orElse("");
//    }
//
//    private <K, S, Q extends HasEntityMeta<K, S>> String fromClause(Q statement) {
//        return "from " + tableName(statement.metaClass());
//    }
//
//    @Override
//    protected  <S, Q extends HasPredicate<S>> String whereClause(Q statement) {
//        return Optional
//                .ofNullable(statement.predicate())
//                .map(this::toConditionClause)
//                .map(cond -> "where " + cond)
//                .orElse("");
//    }
//
//    private <Q extends HasLimit> String limitClause(Q statement) {
//        return Optional.ofNullable(statement.limit())
//                .map(count -> "limit " + count)
//                .orElse("");
//    }
//
//    private <Q extends HasPagination> String skipClause(Q statement) {
//        return Optional.ofNullable(statement.skip())
//                .map(count -> "skip " + count)
//                .orElse("");
//    }
//
//    private <T, Q extends HasSortingInfo<T>> String orderClause(Q statement) {
//        if (statement.sorting().isEmpty()) {
//            return "";
//        }
//
//        StringBuilder builder = new StringBuilder();
//        SortingInfo<T, ?, ?> first = statement.sorting().get(0);
//        builder.append("order by ");
//        builder.append(toOrder(first));
//        statement.sorting().stream().skip(1)
//                .forEach(si -> {
//                    builder.append(", ");
//                    builder.append(toOrder(si));
//                });
//        return builder.toString();
//    }
//
//    private String toOrder(SortingInfo<?, ?, ?> sortingInfo) {
//        return sqlExpressionGenerator.toSqlExpression(sortingInfo.property()) + (sortingInfo.ascending() ? " asc" : " desc");
//    }
//
//    @Override
//    protected  <S> String toConditionClause(ObjectExpression<S, Boolean> condition) {
//        return sqlExpressionGenerator.toSqlExpression(condition);
//    }
//
//    @Override
//    protected  <T> Collection<PropertyExpression<T, ?, ?>> eliminateRedundantProperties(Collection<PropertyExpression<T, ?, ?>> properties) {
//        log.trace("Requested properties: {}", lazy(() -> properties.stream()
//                .map(PropertyExpression::path)
//                .collect(Collectors.joining(", "))));
//
//        Set<PropertyExpression<T, ?, ?>> propertySet = Sets.newLinkedHashSet(properties);
//
//        properties.stream()
//                .flatMap(PropertyExpressions::parentProperties)
//                .peek(p -> log.trace("Removing property {}", p.path()))
//                .forEach(propertySet::remove);
//
//        log.trace("Filtered properties: [{}]", lazy(() -> propertySet.stream()
//                .map(PropertyExpression::path)
//                .collect(Collectors.joining(", "))));
//
//        return propertySet;
//    }
//

}
