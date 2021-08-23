package com.slimgears.rxrepo.sql;

import com.slimgears.nanometer.MetricCollector;
import com.slimgears.rxrepo.query.Repository;
import com.slimgears.rxrepo.query.RepositoryConfigModel;
import com.slimgears.rxrepo.query.provider.QueryProvider;

import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.slimgears.rxrepo.query.provider.QueryProvider.Decorator.of;

public interface SqlServiceFactory {
    SqlStatementProvider statementProvider();
    SqlStatementExecutor statementExecutor();
    SqlSchemaGenerator schemaProvider();
    SqlExpressionGenerator expressionGenerator();
    SqlReferenceResolver referenceResolver();
    QueryProvider queryProvider();
    KeyEncoder keyEncoder();
    SqlTypeMapper typeMapper();
    Supplier<String> dbNameProvider();
    MetricCollector metricCollector();

    abstract class Builder<B extends Builder<B>> {
        private QueryProvider.Decorator decorator = QueryProvider.Decorator.identity();
        protected Function<SqlServiceFactory, SqlStatementExecutor.Decorator> executorDecorator = sf -> SqlStatementExecutor.Decorator.identity();

        protected Function<SqlServiceFactory, SqlStatementProvider> statementProvider;
        protected Function<SqlServiceFactory, SqlStatementExecutor> statementExecutor;
        protected Function<SqlServiceFactory, SqlSchemaGenerator> schemaProvider;
        protected Function<SqlServiceFactory, SqlReferenceResolver> referenceResolver;
        protected Function<SqlServiceFactory, SqlExpressionGenerator> expressionGenerator;
        protected Function<SqlServiceFactory, KeyEncoder> keyEncoder = f -> String::valueOf;
        protected Function<SqlServiceFactory, SqlTypeMapper> typeMapper = f -> SqlTypes.instance;
        protected Function<SqlServiceFactory, Supplier<String>> dbNameProvider;
        protected Function<SqlServiceFactory, QueryProvider> queryProviderGenerator = factory -> new DefaultSqlQueryProvider(
                factory.statementProvider(),
                factory.statementExecutor(),
                factory.schemaProvider(),
                factory.referenceResolver());
        protected Function<SqlServiceFactory, MetricCollector> metricCollector = f -> MetricCollector.empty();

        private Runnable onClose = () -> {};

        @SuppressWarnings("unchecked")
        protected B self() {
            return (B)this;
        }

        public B onClose(Runnable... actions) {
            Runnable oldOnClose = this.onClose;
            this.onClose = () -> {
                oldOnClose.run();
                Stream.of(actions).forEach(Runnable::run);
            };
            return self();
        }

        public B decorateExecutor(Supplier<SqlStatementExecutor.Decorator> decorator) {
            return decorateExecutor((SqlServiceFactory sf) -> decorator.get());
        }

        public B decorateExecutor(Function<SqlServiceFactory, SqlStatementExecutor.Decorator> decorator) {
            Function<SqlServiceFactory, SqlStatementExecutor.Decorator> oldDecorator = this.executorDecorator;
            this.executorDecorator = sf -> oldDecorator.apply(sf).andThen(decorator.apply(sf));
            return self();
        }

        public B decorateExecutorBefore(Supplier<SqlStatementExecutor.Decorator> decorator) {
            return decorateExecutorBefore((SqlServiceFactory sf) -> decorator.get());
        }

        public B decorateExecutorBefore(Function<SqlServiceFactory, SqlStatementExecutor.Decorator> decorator) {
            Function<SqlServiceFactory, SqlStatementExecutor.Decorator> oldDecorator = this.executorDecorator;
            this.executorDecorator = sf -> decorator.apply(sf).andThen(oldDecorator.apply(sf));
            return self();
        }

        public B statementProvider(Function<SqlServiceFactory, SqlStatementProvider> statementProvider) {
            this.statementProvider = statementProvider;
            return self();
        }

        public B statementExecutor(Function<SqlServiceFactory, SqlStatementExecutor> statementExecutor) {
            this.statementExecutor = statementExecutor;
            return self();
        }

        public B schemaProvider(Function<SqlServiceFactory, SqlSchemaGenerator> schemaProvider) {
            this.schemaProvider = schemaProvider;
            return self();
        }

        public B referenceResolver(Function<SqlServiceFactory, SqlReferenceResolver> referenceResolver) {
            this.referenceResolver = referenceResolver;
            return self();
        }

        public B expressionGenerator(Function<SqlServiceFactory, SqlExpressionGenerator> expressionGenerator) {
            this.expressionGenerator = expressionGenerator;
            return self();
        }

        public B queryProviderGenerator(Function<SqlServiceFactory, QueryProvider> queryProviderGenerator) {
            this.queryProviderGenerator = queryProviderGenerator;
            return self();
        }

        public B keyEncoder(Function<SqlServiceFactory, KeyEncoder> keyEncoder) {
            this.keyEncoder = keyEncoder;
            return self();
        }

        public B typeMapper(Function<SqlServiceFactory, SqlTypeMapper> typeMapper) {
            this.typeMapper = typeMapper;
            return self();
        }

        public B dbNameProvider(Function<SqlServiceFactory, Supplier<String>> dbNameProvider) {
            this.dbNameProvider = dbNameProvider;
            return self();
        }

        public abstract SqlServiceFactory build();

        public final Repository buildRepository(RepositoryConfigModel config, QueryProvider.Decorator... decorators) {
            return Repository.fromProvider(this.decorator.apply(build().queryProvider()), config, decorators)
                    .onClose(repo -> onClose.run());
        }

        public final B decorate(QueryProvider.Decorator... decorators) {
            this.decorator = this.decorator.andThen(of(decorators));
            return self();
        }

        public B keyEncoder(Supplier<KeyEncoder> keyEncoder) {
            return keyEncoder(f -> keyEncoder.get());
        }

        public B statementProvider(Supplier<SqlStatementProvider> statementProvider) {
            return statementProvider(f -> statementProvider.get());
        }

        public B statementExecutor(Supplier<SqlStatementExecutor> statementExecutor) {
            return statementExecutor(f -> statementExecutor.get());
        }

        public B schemaProvider(Supplier<SqlSchemaGenerator> schemaProvider) {
            return schemaProvider(f -> schemaProvider.get());
        }

        public B referenceResolver(Supplier<SqlReferenceResolver> referenceResolver) {
            return referenceResolver(f -> referenceResolver.get());
        }

        public B expressionGenerator(Supplier<SqlExpressionGenerator> expressionGenerator) {
            return expressionGenerator(f -> expressionGenerator.get());
        }

        public B typeMapper(Supplier<SqlTypeMapper> typeMapper) {
            return typeMapper(f -> typeMapper.get());
        }

        public B dbName(String dbName) {
            return dbNameProvider(() -> dbName);
        }

        public B dbNameProvider(Supplier<String> dbNameProvider) {
            return dbNameProvider(f -> dbNameProvider);
        }

        public B metricCollector(MetricCollector metricCollector) {
            return metricCollector(() -> metricCollector);
        }

        public B metricCollector(Supplier<MetricCollector> metricCollector) {
            this.metricCollector = f -> metricCollector.get();
            return self();
        }
    }
}
