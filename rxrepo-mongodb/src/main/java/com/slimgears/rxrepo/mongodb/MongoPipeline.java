package com.slimgears.rxrepo.mongodb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;
import com.slimgears.rxrepo.encoding.MetaClassFieldMapper;
import com.slimgears.rxrepo.expressions.*;
import com.slimgears.rxrepo.expressions.internal.MoreTypeTokens;
import com.slimgears.rxrepo.mongodb.adapter.MongoFieldMapper;
import com.slimgears.rxrepo.query.provider.PropertyUpdateInfo;
import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.rxrepo.query.provider.SortingInfo;
import com.slimgears.rxrepo.util.PropertyMetas;
import com.slimgears.rxrepo.util.PropertyReference;
import com.slimgears.rxrepo.util.PropertyReferences;
import com.slimgears.util.autovalue.annotations.MetaClass;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import com.slimgears.util.autovalue.annotations.MetaClasses;
import com.slimgears.util.autovalue.annotations.PropertyMeta;
import com.slimgears.util.generic.MoreStrings;
import com.slimgears.util.stream.Streams;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SuppressWarnings("WeakerAccess")
public class MongoPipeline {
    public final static String aggregationField = "__aggregation";
    public final static String valueField = "__value";
    public final static String versionField = "__version";
    private final static MetaClassFieldMapper fieldMapper = MongoFieldMapper.instance;
    private final static Logger log = LoggerFactory.getLogger(MongoPipeline.class);

    public static Builder builder() {
        return new Builder();
    }

    @SuppressWarnings("WeakerAccess")
    public static class Builder {
        private final ImmutableList.Builder<Document> builder = ImmutableList.builder();

        public Builder match(Document expr) {
            builder.add(new Document("$match", expr));
            return this;
        }

        public Builder match(ObjectExpression<?, ?> expr) {
            return match(expr(expr));
        }

        public Builder lookupAndUnwindReferences(MetaClass<?> metaClass) {
            builder.addAll(PropertyReferences.forMetaClass(metaClass)
                    .stream()
                    .flatMap(pr -> Stream.of(
                            lookup(pr),
                            unwind(pr),
                            exclude(pr.referencePath() +fieldMapper.toReferenceFieldName(pr.property()))))
                    .collect(Collectors.toList()));
            return this;
        }

        public <T> Builder sort(Iterable<SortingInfo<T, ?, ? extends Comparable<?>>> sortingInfos) {
            return sort(toSorting(sortingInfos));
        }

        public Builder sort(Document sortSpec) {
            builder.add(new Document("$sort", sortSpec));
            return this;
        }

        public Builder skip(@Nullable Long skip) {
            Optional.ofNullable(skip)
                    .map(MongoPipeline::skip)
                    .ifPresent(builder::add);
            return this;
        }

        public Builder limit(@Nullable Long limit) {
            Optional.ofNullable(limit)
                    .map(MongoPipeline::limit)
                    .ifPresent(builder::add);
            return this;
        }

        public Builder replaceRoot(ObjectExpression<?, ?> expression) {
            Optional.ofNullable(expression)
                    .filter(exp -> !(exp instanceof ArgumentExpression))
                    .map(MongoPipeline::toExpression)
                    .map(expr -> PropertyMetas.hasMetaClass(expression.reflect().objectType()) ? expr : new Document(valueField, expr))
                    .ifPresent(this::replaceRoot);
            return this;
        }

        public Builder replaceRoot(Object pathExpr) {
            Optional.ofNullable(pathExpr)
                    .map(expr -> new Document("$replaceRoot", new Document("newRoot", expr)))
                    .ifPresent(builder::add);
            return this;
        }

        public Builder distinct() {
            builder.add(
                    new Document("$group", new Document("_id", "$$ROOT")),
                    new Document("$replaceRoot", new Document("newRoot", "$_id")));
            return this;
        }

        public Builder group(Document groupInfo) {
            builder.add(new Document("$group", groupInfo));
            return this;
        }

        public <T> Builder project(@Nullable Iterable<PropertyExpression<T, ?, ?>> properties) {
            Optional.ofNullable(properties)
                    .filter(p -> !Iterables.isEmpty(p))
                    .map(MongoPipeline::toProjection)
                    .map(projection -> new Document("$project", projection))
                    .ifPresent(builder::add);
            return this;
        }

        public <T> Builder aggregate(TypeToken<T> type, Aggregator<T, T, ?> aggregator) {
            Optional.ofNullable(aggregator)
                    .ifPresent(ag -> group(
                            new Document("_id", null)
                            .append(aggregationField, aggregation(type, ag))));
            return this;
        }

        public List<Document> build() {
            return builder.build();
        }
    }

    static <K, S> Document filterFor(MetaClassWithKey<K, S> metaClass, S entity) {
        return filterForKey(metaClass.keyOf(entity));
    }

    static <K> Document filterForKey(K key) {
        return filterForField("_id", key);
    }

    static <T> Document filterForField(String field, T value) {
        return new Document(field, value);
    }

    static <K> Document filterForKeyAndVersion(K key, long version) {
        return filterForKey(key).append(fieldMapper.versionField(), version);
    }


    static <K, S, T> List<Document> aggregationPipeline(QueryInfo<K, S, T> queryInfo, Aggregator<T, T, ?> aggregator) {
        Builder builder = builder();
        builder.lookupAndUnwindReferences(queryInfo.metaClass());

        Optional.ofNullable(queryInfo.predicate())
                .ifPresent(builder::match);

        Optional.of(queryInfo.sorting())
                .filter(si -> !si.isEmpty())
                .ifPresent(builder::sort);

        Optional.ofNullable(queryInfo.skip())
                .ifPresent(builder::skip);

        Optional.ofNullable(queryInfo.limit())
                .ifPresent(builder::limit);

        Optional.ofNullable(queryInfo.mapping())
                .ifPresent(builder::replaceRoot);

        if (Optional.ofNullable(queryInfo.distinct()).orElse(false)) {
            builder.distinct();
        }

        Optional.of(queryInfo.properties())
                .ifPresent(builder::project);

        Optional.ofNullable(aggregator)
                .ifPresent(a -> builder.aggregate(queryInfo.objectType(), a));

        return builder.build();
    }

    static List<Document> aggregationPipeline(QueryInfo<?, ?, ?> queryInfo) {
        return aggregationPipeline(queryInfo, null);
    }

    static Document limit(Long limit) {
        return Optional
                .ofNullable(limit)
                .map(l -> new Document("$limit", l))
                .orElse(null);
    }

    static Document expr(ObjectExpression<?, ?> expr) {
        if (expr == null) {
            return new Document();
        }

        return new Document("$expr", toExpression(expr));
    }

    static <T> Document setFields(ImmutableList<PropertyUpdateInfo<T, ?, ?>> propertyUpdates) {
        Document fields = new Document();
        propertyUpdates.forEach(pu -> fields.append(propertyToString(pu.property()), toExpression(pu.updater())));
        return new Document("$set", fields);
    }

    static <T> Document aggregation(TypeToken<T> type, Aggregator<T, T, ?> aggregator) {
        UnaryOperationExpression<T, Collection<T>, ?> expression = aggregator
                .apply(CollectionExpression.indirectArg(MoreTypeTokens.collection(type)));
        return (Document)new MongoExpressionAdapter().visit(expression, null);
    }

    private static Document skip(Long skip) {
        return Optional.ofNullable(skip)
                .map(s -> new Document("$skip", s))
                .orElse(null);
    }

    private static Object toExpression(ObjectExpression<?, ?> expression) {
        return new MongoExpressionAdapter().visit(expression, null);
    }

    private static Document exclude(String... fields) {
        Document excluded = new Document();
        Arrays.asList(fields).forEach(f -> excluded.append(f, 0));
        return new Document("$project", excluded);
    }

    private static Document lookup(PropertyReference propertyReference) {
        return lookup(propertyReference.referencePath(), propertyReference.property());
    }

    private static Document unwind(PropertyReference propertyReference) {
        return unwind(propertyReference.referencePath(), propertyReference.property());
    }

    private static Document renameReference(PropertyReference reference) {
        return rename(
                reference.referencePath() + fieldMapper.toReferenceFieldName(reference.property()),
                reference.referencePath() + fieldMapper.toFieldName(reference.property()));
    }

    private static Document rename(String fromName, String toName) {
        return new Document("$rename", new Document(fromName, toName));
    }

    private static Document lookup(String prefixPath, PropertyMeta<?, ?> propertyMeta) {
        MetaClassWithKey<?, ?> targetMeta = MetaClasses.forTokenWithKeyUnchecked(propertyMeta.type());
        if (targetMeta == null) {
            throw new RuntimeException(MoreStrings.format("Property {}.{} is not reference property",
                    propertyMeta.declaringType().simpleName(),
                    propertyMeta.name()));
        }
        return new Document("$lookup",
                new Document()
                        .append("from", targetMeta.simpleName())
                        .append("localField", prefixPath + fieldMapper.toReferenceFieldName(propertyMeta))
                        .append("foreignField", "_id")
                        .append("as", prefixPath + fieldMapper.toFieldName(propertyMeta)));
    }

    private static Document unwind(String prefixPath, PropertyMeta<?, ?> propertyMeta) {
        return new Document("$unwind",
                new Document()
                        .append("path", "$" + prefixPath + fieldMapper.toFieldName(propertyMeta))
                        .append("preserveNullAndEmptyArrays", true));
    }

    private static <T> Document toProjection(Iterable<PropertyExpression<T, ?, ?>> properties) {
        Document projection = new Document();
        Set<String> props = Streams.fromIterable(properties)
                .map(MongoPipeline::propertyToString)
                .collect(Collectors.toCollection(TreeSet::new));

        ImmutableSet.copyOf(props)
                .stream()
                .flatMap(MongoPipeline::parentProperties)
                .forEach(props::remove);

        props.forEach(p -> projection.append(p, 1));
        return projection;
    }

    private static Stream<String> parentProperties(String property) {
        int pos = property.lastIndexOf('.');
        if (pos < 0) {
            return Stream.empty();
        }
        String parent = property.substring(0, pos);
        return Stream.concat(Stream.of(parent), parentProperties(parent));
    }

    private static <T> Document toSorting(Iterable<SortingInfo<T, ?, ? extends Comparable<?>>> sortingInfos) {
        Document sorting = new Document();
        sortingInfos.forEach(si -> sorting.append(propertyToString(si.property()), si.ascending() ? 1 : -1));
        return sorting;
    }

    private static String propertyToString(PropertyExpression<?, ?, ?> property) {
        if (property.target().type().operationType() == Expression.OperationType.Property) {
            return propertyToString((PropertyExpression<?, ?, ?>)property.target()) + "." + fieldMapper.toFieldName(property.property());
        }
        return fieldMapper.toFieldName(property.property());
    }

}
