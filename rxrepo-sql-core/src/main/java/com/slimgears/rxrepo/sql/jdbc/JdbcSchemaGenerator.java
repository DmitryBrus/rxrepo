package com.slimgears.rxrepo.sql.jdbc;

import com.slimgears.rxrepo.sql.SqlSchemaGenerator;
import com.slimgears.rxrepo.sql.SqlStatement;
import com.slimgears.rxrepo.sql.SqlStatementExecutor;
import com.slimgears.rxrepo.sql.SqlStatementProvider;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import io.reactivex.Completable;

public class JdbcSchemaGenerator implements SqlSchemaGenerator {
    protected final SqlStatementExecutor statementExecutor;
    protected final SqlStatementProvider statementProvider;

    public JdbcSchemaGenerator(SqlStatementExecutor statementExecutor, SqlStatementProvider statementProvider) {
        this.statementExecutor = statementExecutor;
        this.statementProvider = statementProvider;
    }

    public Completable createDatabase() {
        SqlStatement statement = statementProvider.forCreateSchema();
        return statementExecutor.executeCommand(statement);
    }

    @Override
    public <K, T> Completable createOrUpdate(MetaClassWithKey<K, T> metaClass) {
        SqlStatement statement = statementProvider.forCreateTable(metaClass);
        return statementExecutor.executeCommand(statement);
    }

    @Override
    public void clear() {
        SqlStatement statement = statementProvider.forDropSchema();
        statementExecutor.executeCommand(statement).blockingAwait();
    }
}
