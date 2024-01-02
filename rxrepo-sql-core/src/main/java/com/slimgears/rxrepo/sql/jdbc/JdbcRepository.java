package com.slimgears.rxrepo.sql.jdbc;

import com.slimgears.rxrepo.query.RepositoryConfig;
import com.slimgears.rxrepo.query.decorator.BatchUpdateQueryProviderDecorator;
import com.slimgears.rxrepo.query.decorator.ObserveOnSchedulingQueryProviderDecorator;
import com.slimgears.rxrepo.query.decorator.UpdateReferencesFirstQueryProviderDecorator;
import com.slimgears.rxrepo.sql.*;
import io.reactivex.schedulers.Schedulers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Objects;
import java.util.concurrent.Callable;

public class JdbcRepository {
    public static class Builder<B extends Builder<B>> extends AbstractSqlRepositoryBuilder<B> {
        private Callable<Connection> connectionSupplier;
        private int batchSize = 0;

        public B connection(Callable<Connection> connectionSupplier) {
            this.connectionSupplier = connectionSupplier;
            return self();
        }

        public B connection(String connectionStr) {
            return connection(() -> DriverManager.getConnection(connectionStr));
        }

        public B enableBatch(int batchSize) {
            this.batchSize = batchSize;
            return self();
        }

        @Override
        protected SqlServiceFactory.Builder<?> serviceFactoryBuilder(RepositoryConfig config) {
            Objects.requireNonNull(connectionSupplier);
            return serviceFactoryBuilder(config, connectionSupplier);
        }

        protected SqlServiceFactory.Builder<?> serviceFactoryBuilder(RepositoryConfig config, Callable<Connection> connectionSupplier) {
            return DefaultSqlServiceFactory.builder()
                    .keyEncoder(DigestKeyEncoder::create)
                    .expressionGenerator(DefaultSqlExpressionGenerator::new)
                    .dbNameProvider(() -> "repository")
                    .typeMapper(() -> SqlTypes.instance)
                    .schemaProvider(sf -> new JdbcSchemaGenerator(sf.statementExecutor(), sf.statementProvider()))
                    .referenceResolver(sf -> new DefaultSqlReferenceResolver(sf.keyEncoder(), sf.expressionGenerator()))
                    .statementProvider(sf -> new DefaultSqlStatementProvider(sf.expressionGenerator(), sf.typeMapper(), sf.dbNameProvider()))
                    .statementExecutor(sf -> new JdbcSqlStatementExecutor(connectionSupplier, sf.typeMapper()))
                    .decorateExecutor(sf -> JdbcSqlStatementExecutorDecorator.create(sf.typeMapper(), sf.keyEncoder()))
                    .decorate(
//                            LockQueryProviderDecorator.create(SemaphoreLockProvider.create()),
//                            LiveQueryProviderDecorator.create(Duration.ofMillis(config.aggregationDebounceTimeMillis())),
                            ObserveOnSchedulingQueryProviderDecorator.create(Schedulers.io()),
                            BatchUpdateQueryProviderDecorator.create(batchSize),
                            UpdateReferencesFirstQueryProviderDecorator.create()
                    );
        }
    }

    public static Builder<?> builder() {
        return new Builder<>();
    }
}
