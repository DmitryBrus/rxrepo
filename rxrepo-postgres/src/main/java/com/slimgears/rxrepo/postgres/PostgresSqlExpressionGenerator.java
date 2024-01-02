package com.slimgears.rxrepo.postgres;

import com.slimgears.rxrepo.expressions.Expression;
import com.slimgears.rxrepo.sql.DefaultSqlExpressionGenerator;
import com.slimgears.rxrepo.util.ExpressionTextGenerator;

public class PostgresSqlExpressionGenerator extends DefaultSqlExpressionGenerator {
    @Override
    protected ExpressionTextGenerator.Builder createBuilder() {
        return super.createBuilder()
                .add(Expression.Type.CollectionArgument, "*");
    }
}
