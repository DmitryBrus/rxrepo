package com.slimgears.rxrepo.sql;

import com.slimgears.rxrepo.query.Repository;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import io.reactivex.Completable;
import io.reactivex.Scheduler;

import java.util.function.Function;
import java.util.function.Supplier;

public interface SqlServiceFactory {
    SqlStatementProvider statementProvider();
    SqlStatementExecutor statementExecutor();
    SchemaProvider schemaProvider();
    SqlExpressionGenerator expressionGenerator();
    SqlAssignmentGenerator assignmentGenerator();
    Scheduler scheduler();
    Completable shutdownSignal();
    ReferenceResolver referenceResolver();
    QueryProvider queryProvider();

    static Builder builder() {
        return DefaultSqlServiceFactory.builder();
    }

    interface Builder {
        Builder statementProvider(Function<SqlServiceFactory, SqlStatementProvider> statementProvider);
        Builder statementExecutor(Function<SqlServiceFactory, SqlStatementExecutor> statementExecutor);
        Builder schemaProvider(Function<SqlServiceFactory, SchemaProvider> schemaProvider);
        Builder referenceResolver(Function<SqlServiceFactory, ReferenceResolver> referenceResolver);
        Builder expressionGenerator(Function<SqlServiceFactory, SqlExpressionGenerator> expressionGenerator);
        Builder assignmentGenerator(Function<SqlServiceFactory, SqlAssignmentGenerator> assignmentGenerator);
        Builder scheduler(Scheduler scheduler);
        Builder shutdownSignal(Completable shutdown);
        SqlServiceFactory build();

        default Repository buildRepository() {
            return Repository.fromProvider(build().queryProvider());
        }

        default Builder statementProvider(Supplier<SqlStatementProvider> statementProvider) {
            return statementProvider(f -> statementProvider.get());
        }

        default Builder statementExecutor(Supplier<SqlStatementExecutor> statementExecutor) {
            return statementExecutor(f -> statementExecutor.get());
        }

        default Builder schemaProvider(Supplier<SchemaProvider> schemaProvider) {
            return schemaProvider(f -> schemaProvider.get());
        }

        default Builder referenceResolver(Supplier<ReferenceResolver> referenceResolver) {
            return referenceResolver(f -> referenceResolver.get());
        }

        default Builder expressionGenerator(Supplier<SqlExpressionGenerator> expressionGenerator) {
            return expressionGenerator(f -> expressionGenerator.get());
        }

        default Builder assignmentGenerator(Supplier<SqlAssignmentGenerator> assignmentGenerator) {
            return assignmentGenerator(f -> assignmentGenerator.get());
        }
    }
}
