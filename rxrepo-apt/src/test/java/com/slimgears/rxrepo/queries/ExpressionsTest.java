package com.slimgears.rxrepo.queries;

import com.slimgears.rxrepo.annotations.Filterable;
import com.slimgears.rxrepo.annotations.Indexable;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.filters.ComparableFilter;
import com.slimgears.rxrepo.util.Expressions;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static com.slimgears.rxrepo.filters.ComparableFilter.fromGreaterOrEqual;
import static com.slimgears.rxrepo.filters.ComparableFilter.fromLessThan;
import static com.slimgears.rxrepo.filters.StringFilter.fromContains;
import static com.slimgears.rxrepo.queries.TestEntities.*;


public class ExpressionsTest {
    @Test
    public void testPropertyExpressionCompile() {
        String description = Expressions.compile(TestEntity.$.text).apply(testEntity1);
        Assert.assertEquals(testEntity1.text(), description);
    }

    @Test
    public void testReferencePropertyExpressionCompile() {
        ObjectExpression<TestEntity, String> exp = TestEntity.$.refEntity.text;
        String description = Expressions.compile(exp).apply(testEntity1);
        Assert.assertEquals(description, testEntity1.refEntity().text());
    }

    @Test
    public void testComparisonExpression() {
        Function<TestEntity, Boolean> exp = Expressions.compile(TestEntity.$.text.length().eq(TestEntity.$.number));
        Assert.assertFalse(exp.apply(testEntity1));
        Assert.assertTrue(exp.apply(testEntity2));
    }

    @Test
    public void testMathExpression() {
        Function<TestEntity, Integer> exp = Expressions
                .compile(TestEntity.$.number
                        .add(TestEntity.$.text.length())
                        .add(TestEntity.$.refEntity.id)
                        .mul(100)
                        .div(5));

        Assert.assertEquals(Integer.valueOf(420), exp.apply(testEntity1));
        Assert.assertEquals(Integer.valueOf(520), exp.apply(testEntity2));
    }

    @Test
    public void testAsStringExpression() {
        Function<TestEntity, String> exp = Expressions
                .compile(TestEntity.$.number.asString()
                        .concat(" - ")
                        .concat(TestEntity.$.refEntity.text)
                        .concat(" - ")
                        .concat(TestEntity.$.text));
        Assert.assertEquals("3 - Description 1 - Entity 1", exp.apply(testEntity1));
        Assert.assertEquals("8 - Description 2 - Entity 2", exp.apply(testEntity2));
    }

    @Test
    public void testFilterToExpression() {
        TestEntity.Filter filter = TestEntity.Filter.builder()
                .refEntity(TestRefEntity.Filter.builder().id(fromGreaterOrEqual(8)).build())
                .number(fromLessThan(5))
                .text(fromContains("ity 1"))
                .searchText("ity")
                .build();

        Function<TestEntity, Boolean> func = filter.toExpression(ObjectExpression.arg(TestEntity.class))
                .map(Expressions::compile)
                .orElse(e -> false);

        Assert.assertTrue(func.apply(testEntity1));
        Assert.assertFalse(func.apply(testEntity2));
    }

    @Test
    public void testNestedFilterToExpression() {
        TestRefEntity inner = TestRefEntity.create(1, "", null);
        TestRefEntity refWithInner = TestRefEntity.create(2, "", inner);
        TestRefEntity.Filter filter = TestRefEntity.Filter.builder()
                .testRefEntity(TestRefEntity.Filter.builder().id(ComparableFilter.fromEqualsTo(1)).build())
                .build();

        Function<TestRefEntity, Boolean> func = filter.toExpression(ObjectExpression.arg(TestRefEntity.class))
                .map(Expressions::compile)
                .orElse(e -> false);

        Assert.assertTrue(func.apply(refWithInner));
        Assert.assertFalse(func.apply(inner));
    }

    @Test
    public void testAnnotationRetrieval() {
        Assert.assertTrue(TestEntity.metaClass.text.hasAnnotation(Filterable.class));
        Assert.assertTrue(TestEntity.metaClass.number.hasAnnotation(Indexable.class));
        Assert.assertFalse(TestEntity.metaClass.refEntity.hasAnnotation(Indexable.class));
    }

    @Test
    public void testValueInExpression() {
        List<String> strings = Collections.singletonList("Entity 1");
        Function<TestEntity, Boolean> exp = Expressions
                .compile(TestEntity.$.text.in(strings));

        Assert.assertTrue(exp.apply(testEntity1));
        Assert.assertFalse(exp.apply(testEntity2));
    }

    @Test
    public void testNullExpression() {
        Function<TestEntity, Boolean> exp = Expressions
                .compile(TestEntity.$.address.isNull());

        Assert.assertTrue(exp.apply(testEntity1));
        Assert.assertFalse(exp.apply(testEntity2));
    }

    @Test
    public void testWhenEqGetNullPropertyValue() {
        Function<TestEntity, Boolean> exp = Expressions
                .compile(TestEntity.$.address.eq("Address"));

        Assert.assertFalse(exp.apply(testEntity1));
        Assert.assertTrue(exp.apply(testEntity2));
    }

    @Test
    public void testAndEvaluation() {
        Function<TestEntity, Boolean> exp = Expressions
                .compile(TestEntity.$.address.isNotNull().and(TestEntity.$.address.eq("Address")));

        Assert.assertFalse(exp.apply(testEntity1));
    }

    @Test
    public void testOrEvaluation() {
        Function<TestEntity, Boolean> exp = Expressions
                .compile(TestEntity.$.address.isNull().or(TestEntity.$.address.eq("Address")));

        Assert.assertTrue(exp.apply(testEntity1));
        Assert.assertTrue(exp.apply(testEntity2));
    }

    //Null handling tests
    @Test
    public void testAsStringWithNull() {
        Function<TestEntity, String> exp = Expressions
                .compile(TestEntity.$.code.asString());

        Assert.assertNull(exp.apply(testEntity1));
    }

    @Test
    public void testAddWithNull1() {
        Function<TestEntity, Integer> exp = Expressions
                .compile(TestEntity.$.code.add(TestEntity.$.number));

        Assert.assertEquals(Integer.valueOf(3), exp.apply(testEntity1));
    }

    @Test
    public void testAddWithNull2() {
        Function<TestEntity, Integer> exp = Expressions
                .compile(TestEntity.$.number.add(TestEntity.$.code));

        Assert.assertEquals(Integer.valueOf(3), exp.apply(testEntity1));
    }

    @Test
    public void testSubtractWithNull1() {
        Function<TestEntity, Integer> exp = Expressions
                .compile(TestEntity.$.code.sub(TestEntity.$.number));

        Assert.assertEquals(Integer.valueOf(-3), exp.apply(testEntity1));
    }

    @Test
    public void testSubtractWithNull2() {
        Function<TestEntity, Integer> exp = Expressions
                .compile(TestEntity.$.number.sub(TestEntity.$.code));

        Assert.assertEquals(Integer.valueOf(3), exp.apply(testEntity1));
    }


    @Test
    public void testMultiplyWithNull1() {
        Function<TestEntity, Integer> exp = Expressions
                .compile(TestEntity.$.code.mul(TestEntity.$.number));

        Assert.assertEquals(Integer.valueOf(0), exp.apply(testEntity1));
    }

    @Test
    public void testMultiplyWithNull2() {
        Function<TestEntity, Integer> exp = Expressions
                .compile(TestEntity.$.number.mul(TestEntity.$.code));

        Assert.assertEquals(Integer.valueOf(0), exp.apply(testEntity1));
    }

    @Test
    public void testDivideWithNull1() {
        Function<TestEntity, Integer> exp = Expressions
                .compile(TestEntity.$.code.div(TestEntity.$.number));

        Assert.assertEquals(Integer.valueOf(0), exp.apply(testEntity1));
    }

    @Test
    public void testDivideWithNull2() {
        Function<TestEntity, Integer> exp = Expressions
                .compile(TestEntity.$.number.div(TestEntity.$.code));

        Assert.assertEquals(Integer.valueOf(3), exp.apply(testEntity1));
    }

    @Test
    public void testNegateWithNull() {
        Function<TestEntity, Integer> exp = Expressions
                .compile(TestEntity.$.code.negate());

        Assert.assertNull(exp.apply(testEntity1));
    }

    @Test
    public void testEqualsWithNull1() {
        Function<TestEntity, Boolean> exp = Expressions
                .compile(TestEntity.$.code.eq(TestEntity.$.number));

        Assert.assertFalse(exp.apply(testEntity1));
    }

    @Test
    public void testEqualsWithNull2() {
        Function<TestEntity, Boolean> exp = Expressions
                .compile(TestEntity.$.number.eq(TestEntity.$.code));

        Assert.assertFalse(exp.apply(testEntity1));
    }

    @Test
    public void testEqualsWithNull3() {
        Function<TestEntity, Boolean> exp = Expressions
                .compile(TestEntity.$.code.eq(TestEntity.$.code));

        Assert.assertTrue(exp.apply(testEntity1));
    }

    @Test
    public void testGreaterThanWithNull1() {
        Function<TestEntity, Boolean> exp = Expressions
                .compile(TestEntity.$.code.greaterThan(TestEntity.$.number));

        Assert.assertFalse(exp.apply(testEntity1));
    }

    @Test
    public void testGreaterThanWithNull2() {
        Function<TestEntity, Boolean> exp = Expressions
                .compile(TestEntity.$.number.greaterThan(TestEntity.$.code));

        Assert.assertFalse(exp.apply(testEntity1));
    }

    @Test
    public void testGreaterThanWithNull3() {
        Function<TestEntity, Boolean> exp = Expressions
                .compile(TestEntity.$.code.greaterThan(TestEntity.$.code));

        Assert.assertFalse(exp.apply(testEntity1));
    }

    @Test
    public void testLessThanWithNull1() {
        Function<TestEntity, Boolean> exp = Expressions
                .compile(TestEntity.$.code.lessThan(TestEntity.$.number));

        Assert.assertFalse(exp.apply(testEntity1));
    }

    @Test
    public void testLessThanWithNull2() {
        Function<TestEntity, Boolean> exp = Expressions
                .compile(TestEntity.$.number.lessThan(TestEntity.$.code));

        Assert.assertFalse(exp.apply(testEntity1));
    }

    @Test
    public void testLessThanWithNull3() {
        Function<TestEntity, Boolean> exp = Expressions
                .compile(TestEntity.$.code.lessThan(TestEntity.$.code));

        Assert.assertFalse(exp.apply(testEntity1));
    }

    @Test
    public void testIsEmptyWithNull() {
        Function<TestEntity, Boolean> exp = Expressions
                .compile(TestEntity.$.address.isEmpty());

        Assert.assertTrue(exp.apply(testEntity1));
        Assert.assertFalse(exp.apply(testEntity2));
    }

    @Test
    public void testContainsWithNull1() {
        Function<TestEntity, Boolean> exp = Expressions
                .compile(TestEntity.$.address.contains("A"));

        Assert.assertFalse(exp.apply(testEntity1));
        Assert.assertTrue(exp.apply(testEntity2));
    }

    @Test
    public void testContainsWithNull2() {
        Function<TestEntity, Boolean> exp = Expressions
                .compile(TestEntity.$.text.contains(TestEntity.$.address));

        Assert.assertTrue(exp.apply(testEntity1));
    }

    @Test
    public void testStartsWithWithNull1() {
        Function<TestEntity, Boolean> exp = Expressions
                .compile(TestEntity.$.address.startsWith("A"));

        Assert.assertFalse(exp.apply(testEntity1));
        Assert.assertTrue(exp.apply(testEntity2));
    }

    @Test
    public void testStartsWithWithNull2() {
        Function<TestEntity, Boolean> exp = Expressions
                .compile(TestEntity.$.text.startsWith(TestEntity.$.address));

        Assert.assertTrue(exp.apply(testEntity1));
    }

    @Test
    public void testEndsWithWithNull1() {
        Function<TestEntity, Boolean> exp = Expressions
                .compile(TestEntity.$.address.endsWith("ss"));

        Assert.assertFalse(exp.apply(testEntity1));
        Assert.assertTrue(exp.apply(testEntity2));
    }

    @Test
    public void testEndsWithWithNull2() {
        Function<TestEntity, Boolean> exp = Expressions
                .compile(TestEntity.$.text.endsWith(TestEntity.$.address));

        Assert.assertTrue(exp.apply(testEntity1));
    }

    @Test
    public void testMatchesWithNull1() {
        Function<TestEntity, Boolean> exp = Expressions
                .compile(TestEntity.$.address.matches("^[Aa]"));

        Assert.assertFalse(exp.apply(testEntity1));
    }

    @Test
    public void testMatchesWithWithNull2() {
        Function<TestEntity, Boolean> exp = Expressions
                .compile(TestEntity.$.text.matches(TestEntity.$.address));

        Assert.assertFalse(exp.apply(testEntity1));
    }

    @Test
    public void testLengthWithNull() {
        Function<TestEntity, Integer> exp = Expressions
                .compile(TestEntity.$.address.length());

        Assert.assertEquals(Integer.valueOf(0),exp.apply(testEntity1));
    }

    @Test
    public void testConcatWithNull1() {
        Function<TestEntity, String> exp = Expressions
                .compile(TestEntity.$.address.concat(TestEntity.$.text));

        Assert.assertEquals(textEntity1, exp.apply(testEntity1));
    }

    @Test
    public void testConcatWithNull2() {
        Function<TestEntity, String> exp = Expressions
                .compile(TestEntity.$.text.concat(TestEntity.$.address));

        Assert.assertEquals(textEntity1, exp.apply(testEntity1));
    }

    @Test
    public void testToLowerWithNull() {
        Function<TestEntity, String> exp = Expressions
                .compile(TestEntity.$.address.toLower());

        Assert.assertNull(exp.apply(testEntity1));
    }

    @Test
    public void testToUpperWithNull() {
        Function<TestEntity, String> exp = Expressions
                .compile(TestEntity.$.address.toUpper());

        Assert.assertNull(exp.apply(testEntity1));
    }

    @Test
    public void testTrimWithNull() {
        Function<TestEntity, String> exp = Expressions
                .compile(TestEntity.$.address.trim());

        Assert.assertNull(exp.apply(testEntity1));
    }

    @Test
    public void testSearchTextWithNull1() {
        Function<TestEntity, Boolean> exp = Expressions
                .compile(TestEntity.$.address.searchText("Ad"));

        Assert.assertFalse(exp.apply(testEntity1));
    }

    @Test
    public void testValueInTextWithNull1() {
        Function<TestEntity, Boolean> exp = Expressions
                .compile(TestEntity.$.address.in());

        Assert.assertFalse(exp.apply(testEntity2));
    }

    @Test
    public void testValueInTextWithNull2() {
        Function<TestEntity, Boolean> exp = Expressions
                .compile(TestEntity.$.text.in(TestEntity.$.col));

        Assert.assertFalse(exp.apply(testEntity1));
    }

    @Test
    public void testExpressionWithReferenceToNestedNullableField() {
        Function<TestEntity, String> exp = Expressions
                .compile(TestEntity.$.optionalRefEntity.text);

        Assert.assertNull(exp.apply(testEntity1));
    }
}
