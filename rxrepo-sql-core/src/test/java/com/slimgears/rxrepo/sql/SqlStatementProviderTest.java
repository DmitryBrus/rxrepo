package com.slimgears.rxrepo.sql;

import com.google.common.collect.ImmutableSet;
import com.slimgears.rxrepo.query.provider.DeleteInfo;
import com.slimgears.rxrepo.query.provider.PropertyUpdateInfo;
import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.rxrepo.query.provider.UpdateInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;

public class SqlStatementProviderTest {
    private SqlStatementProvider statementProvider;
    private SqlReferenceResolver referenceResolver;

    @Before
    public void setUp() {
        SqlExpressionGenerator expressionGenerator = new DefaultSqlExpressionGenerator();
        statementProvider = new DefaultSqlStatementProvider(expressionGenerator, SqlTypes.instance, () -> "repository");
        referenceResolver = new DefaultSqlReferenceResolver(DigestKeyEncoder.create(), expressionGenerator);
    }

    @Test
    public void testQueryStatementGeneration() {
        SqlStatement statement = statementProvider.forQuery(QueryInfo.<Integer, Product, Product>builder()
                .metaClass(Product.metaClass)
                .predicate(Product.$.name.contains("substr")
                        .and(Product.$.price.lessThan(100))
                        .and(Product.$.type.in(ProductPrototype.Type.ComputeHardware, ProductPrototype.Type.ComputerSoftware)))
                .properties(ImmutableSet.of(Product.$.name, Product.$.price, Product.$.id))
                .sortAscending(Product.$.name)
                .sortDescending(Product.$.id)
                .limit(100L)
                .skip(200L)
                .build());

        Assert.assertEquals(
                "select \"name\", \"price\", \"id\" from repository.Product " +
                        "where (((\"name\" like '%' + ? + '%') and (\"price\" < ?)) and (\"type\" in (?))) " +
                        "order by \"name\" asc, \"id\" desc " +
                        "limit 100 " +
                        "skip 200",
                statement.statement());
        Assert.assertArrayEquals(statement.args(),
                new Object[]{"substr", 100, Arrays.asList(ProductPrototype.Type.ComputeHardware, ProductPrototype.Type.ComputerSoftware)});
    }

    @Test
    public void testQueryWithMappingStatementGeneration() {
        SqlStatement statement = statementProvider.forQuery(QueryInfo.<Integer, Product, Integer>builder()
                .metaClass(Product.metaClass)
                .mapping(Product.$.inventory.name.length().add(5))
                .predicate(Product.$.name.contains("substr").and(Product.$.price.lessThan(100)))
                .limit(100L)
                .skip(200L)
                .build());

        Assert.assertEquals(
                "select (LEN(\"inventory.name\") + ?) " +
                        "from repository.Product " +
                        "where ((\"name\" like '%' + ? + '%') and (\"price\" < ?)) " +
                        "limit 100 " +
                        "skip 200", statement.statement());
        Assert.assertArrayEquals(new Object[]{5, "substr", 100}, statement.args());
    }

    @Test
    public void testInsertOrUpdateStatementGeneration() {
        Product product = Product.builder()
                .id(200)
                .name("prd1")
                .price(30)
                .inventory(Inventory.builder()
                        .id(300)
                        .name("inv1")
                        .build())
                .type(ProductPrototype.Type.ComputeHardware)
                .build();

        SqlStatement statement = statementProvider.forInsertOrUpdate(Product.metaClass, product, referenceResolver);
        Assert.assertEquals(
                "insert into repository.Product " +
                        "(\"id\", \"name\", \"inventory\", \"type\", \"price\") " +
                        "values (?, ?, (?), ?, ?)\n" +
                        "on conflict(id) do\n" +
                        "update  set \"id\" = ?, \"name\" = ?, \"inventory\" = ?, \"type\" = ?, \"price\" = ?",
                statement.statement());
        Assert.assertArrayEquals(new Object[] { 200, "prd1", 300, ProductPrototype.Type.ComputeHardware, 30, 200, "prd1", product.inventory(), ProductPrototype.Type.ComputeHardware, 30}, statement.args());
    }

    @Test
    public void testDeleteStatementGeneration() {
        SqlStatement statement = statementProvider.forDelete(DeleteInfo.<Integer, Product>builder()
                .metaClass(Product.metaClass)
                .limit(100L)
                .predicate(Product.$.name.greaterOrEqual("product1"))
                .build());
        Assert.assertEquals("delete from repository.Product where (not (\"name\" < ?)) limit 100", statement.statement());
        Assert.assertArrayEquals(new Object[]{"product1"}, statement.args());
    }

    @Test
    public void testUpdateStatementGeneration() {
        SqlStatement statement = statementProvider.forUpdate(UpdateInfo.<Integer, Product>builder()
                .metaClass(Product.metaClass)
                .propertyUpdatesAdd(PropertyUpdateInfo.create(Product.$.name, Product.$.name.concat("aa")))
                .predicate(Product.$.name.contains("bbb"))
                .limit(100L)
                .build());
        Assert.assertEquals(
                "update repository.Product " +
                        "set \"name\" = (\"name\" || ?) " +
                        "where (\"name\" like '%' + ? + '%') " +
                        "limit 100",
                statement.statement());
        Assert.assertArrayEquals(new Object[]{"aa", "bbb"}, statement.args());
    }
}
