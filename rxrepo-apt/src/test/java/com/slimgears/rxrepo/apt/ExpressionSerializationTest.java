package com.slimgears.rxrepo.apt;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.slimgears.rxrepo.expressions.BooleanExpression;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.expressions.internal.ExpressionModule;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class ExpressionSerializationTest {
    private ObjectMapper objectMapper;

    @Before
    public void setUp() {
        objectMapper = new ObjectMapper().registerModule(new ExpressionModule());
    }

    @Test
    public void testSerialization() throws IOException {
        ObjectExpression<TestEntity<?>, ?> intExpression =
                TestEntity.$.text
                .concat(TestEntity.$.description)
                .length()
                .add(5);

        String json = objectMapper.writeValueAsString(intExpression);
        Assert.assertNotNull(json);
        System.out.println(json);

        ObjectExpression<TestEntity<?>, ? extends Comparable<?>> deserializedExpression = objectMapper.readValue(json, new TypeReference<ObjectExpression<TestEntity<? extends Comparable<?>>, ? extends Comparable<?>>>(){});
        Assert.assertNotNull(deserializedExpression);
        Assert.assertEquals(objectMapper.writeValueAsString(intExpression), objectMapper.writeValueAsString(deserializedExpression));

        BooleanExpression<TestEntity<?>> expression = BooleanExpression.and(
                TestEntity.$.text.eq("5"),
                TestEntity.$.referencedEntity.description.eq("22"),
                TestEntity.$.referencedEntity.text.startsWith("3"));

        System.out.println(objectMapper.writeValueAsString(expression));
    }
}
