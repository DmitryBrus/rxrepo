package com.slimgears.rxrepo.orientdb;

import com.google.common.collect.ImmutableMap;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.slimgears.nanometer.MetricCollector;
import com.slimgears.rxrepo.query.RepositoryConfig;
import com.slimgears.rxrepo.query.decorator.*;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.rxrepo.sql.*;
import com.slimgears.util.stream.Lazy;
import com.slimgears.util.stream.Safe;
import io.reactivex.schedulers.Schedulers;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.function.Function;

public class OrientDbRepository {
    public enum Type {
        Memory,
        Persistent
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends AbstractSqlRepositoryBuilder<Builder> {
        private final static int pageSize = 64 * 1024;
        private final static Object lock = new Object();
        private final static ImmutableMap<Type, ODatabaseType> dbTypeMap = ImmutableMap
                .<Type, ODatabaseType>builder()
                .put(Type.Memory, ODatabaseType.MEMORY)
                .put(Type.Persistent, ODatabaseType.PLOCAL)
                .build();
        private String url = "embedded:repository";

        private String dbName = "repository";
        private ODatabaseType dbType = ODatabaseType.MEMORY;
        private String user = "admin";
        private String password = "admin";
        private String serverUser = "root";
        private String serverPassword = Optional.ofNullable(System.getenv("ORIENTDB_ROOT_PASSWORD")).orElse("root");
        private boolean batchSupport = false;
        private int batchBufferSize = 2000;
        private QueryProvider.Decorator decorator = QueryProvider.Decorator.identity();
        private Function<Executor, Executor> executorDecorator = Function.identity();

        private final Map<OGlobalConfiguration, Object> customConfig = new HashMap<>();
        private int maxConnections = 12;

        public final Builder enableBatchSupport() {
            return enableBatchSupport(true);
        }

        public final Builder enableBatchSupport(boolean enable) {
            this.batchSupport = enable;
            return this;
        }

        public final Builder enableBatchSupport(int bufferSize) {
            this.batchSupport = true;
            this.batchBufferSize = bufferSize;
            return this;
        }

        public final Builder maxConnections(int maxConnections) {
            this.maxConnections = maxConnections;
            return this;
        }

        public final Builder maxNonHeapMemory(int maxNonHeapMemoryBytes) {
            this.customConfig.put(OGlobalConfiguration.DIRECT_MEMORY_POOL_LIMIT, maxNonHeapMemoryBytes / pageSize);
            return this;
        }

        public final Builder decorateExecutor(Function<Executor, Executor> executorDecorator) {
            this.executorDecorator = this.executorDecorator.andThen(executorDecorator);
            return this;
        }

        public final Builder setProperty(String key, Object value) {
            customConfig.put(OGlobalConfiguration.findByKey(key), value);
            return this;
        }

        public final Builder setProperties(@Nonnull Map<String, Object> properties) {
            properties.forEach(this::setProperty);
            return this;
        }

        public final Builder url(@Nonnull String url) {
            this.url = url;
            return this;
        }

        public final Builder type(@Nonnull Type type) {
            this.dbType = dbTypeMap.get(type);
            return this;
        }

        public final Builder name(String dbName) {
            this.dbName = dbName;
            return this;
        }

        public final Builder user(@Nonnull String user) {
            this.user = user;
            return this;
        }

        public final Builder password(@Nonnull String password) {
            this.password = password;
            return this;
        }

        public final Builder serverUser(@Nonnull String serverUser) {
            this.serverUser = serverUser;
            return this;
        }

        public final Builder serverPassword(@Nonnull String serverPassword) {
            this.serverPassword = serverPassword;
            return this;
        }

        public final Builder decorate(@Nonnull QueryProvider.Decorator... decorators) {
            this.decorator = this.decorator.andThen(QueryProvider.Decorator.of(decorators));
            return this;
        }

        private OrientDB createClient(String url, String serverUser, String serverPassword, String dbName, ODatabaseType dbType) {
            OrientDBConfig config = OrientDBConfig.builder()
                    .fromGlobalMap(customConfig)
                    .build();

            OrientDB client = new OrientDB(url, serverUser, serverPassword, config);
            if (!client.exists(dbName)) {
                synchronized (lock) {
                    client.createIfNotExists(dbName, dbType);
                }
            }
            return client;
        }

        @Override
        public Builder retryCount(int value) {
            configBuilder.retryCount(value);
            return this;
        }

        @Override
        public Builder bufferDebounceTimeoutMillis(int value) {
            configBuilder.bufferDebounceTimeoutMillis(value);
            return this;
        }

        @Override
        public Builder aggregationDebounceTimeMillis(int value) {
            configBuilder.aggregationDebounceTimeMillis(value);
            return this;
        }

        @Override
        public Builder retryInitialDurationMillis(int value) {
            configBuilder.retryInitialDurationMillis(value);
            return this;
        }

        public Builder enableMetrics(MetricCollector metricCollector) {
            MetricsQueryProviderDecorator decorator = MetricsQueryProviderDecorator.create(metricCollector);
            decorate(decorator);
            decorateExecutor(decorator.executorDecorator());
            return this;
        }

        private SqlServiceFactory.Builder<?> serviceFactoryBuilder(OrientDbSessionProvider dbSessionProvider) {
            return DefaultSqlServiceFactory.builder()
                    .schemaProvider(svc -> new OrientDbSqlSchemaGenerator(dbSessionProvider))
                    .statementExecutor(svc -> OrientDbMappingStatementExecutor.decorate(new OrientDbStatementExecutor(dbSessionProvider), svc.keyEncoder()))
                    .expressionGenerator(svc -> OrientDbSqlExpressionGenerator.create(svc.keyEncoder()))
                    .dbNameProvider(Lazy.of(() -> dbSessionProvider.session().map(ODatabaseDocument::getName).blockingGet()))
                    .statementProvider(svc -> new OrientDbSqlStatementProvider(
                            svc.expressionGenerator(),
                            svc.typeMapper(),
                            svc.keyEncoder(),
                            svc.dbNameProvider()))
                    .referenceResolver(svc -> new OrientDbSqlReferenceResolver(svc.statementProvider()))
                    .queryProviderGenerator(svc -> batchSupport ? OrientDbQueryProvider.create(svc, dbSessionProvider, batchBufferSize) : DefaultSqlQueryProvider.create(svc))
                    .keyEncoder(DigestKeyEncoder::create);
        }

        @Override
        protected SqlServiceFactory.Builder<?> serviceFactoryBuilder(RepositoryConfig config) {
            Lazy<OrientDB> dbClient = Lazy.of(() -> createClient(url, serverUser, serverPassword, dbName, dbType));
            OrientDbSessionProvider dbSessionProvider = OrientDbSessionProvider.create(() -> dbClient.get().open(dbName, user, password), maxConnections);

            return serviceFactoryBuilder(dbSessionProvider)
                    .onClose(Safe.ofRunnable(dbSessionProvider::close), dbClient::close)
                    .decorate(
                            RetryOnConcurrentConflictQueryProviderDecorator.create(Duration.ofMillis(config.retryInitialDurationMillis()), config.retryCount()),
                            OrientDbUpdateReferencesFirstQueryProviderDecorator.create(),
                            BatchUpdateQueryProviderDecorator.create(batchBufferSize),
                            LiveQueryProviderDecorator.create(Duration.ofMillis(config.aggregationDebounceTimeMillis())),
                            ObserveOnSchedulingQueryProviderDecorator.create(Schedulers.io()),
                            OrientDbDropDatabaseQueryProviderDecorator.create(dbClient, dbName),
                            SubscribeOnSchedulingQueryProviderDecorator.createDefault(),
                            decorator);
        }
    }
}
