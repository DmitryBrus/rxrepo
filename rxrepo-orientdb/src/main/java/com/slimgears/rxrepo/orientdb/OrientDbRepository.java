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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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
        private final static long pageSize = 64 * 1024;
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
        private QueryProvider.Decorator preDecorator = QueryProvider.Decorator.identity();
        private QueryProvider.Decorator postDecorator = QueryProvider.Decorator.identity();
        private Function<Executor, Executor> executorDecorator = Function.identity();
        private SqlStatementExecutor.Decorator sqlExecutorDecorator = SqlStatementExecutor.Decorator.identity();

        private final Map<OGlobalConfiguration, Object> customConfig = new HashMap<>();
        private int maxUpdateConnections = 12;
        private int maxQueryConnections = 12;
        private Duration cacheExpirationTime = Duration.ofMinutes(2);
        private MetricCollector metricCollector = MetricCollector.empty();
        private OrientDbProvider orientDbProvider;

        public final Builder enableBatchSupport() {
            return enableBatchSupport(true);
        }

        public Builder orientDbProvider(OrientDbProvider orientDbProvider) {
            this.orientDbProvider = orientDbProvider;
            return this;
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
            this.maxUpdateConnections = maxConnections / 2;
            this.maxQueryConnections = maxConnections / 2;
            return this;
        }

        public final Builder maxUpdateConnections(int maxConnections) {
            this.maxUpdateConnections = maxConnections;
            return this;
        }

        public final Builder maxQueryConnections(int maxConnections) {
            this.maxQueryConnections = maxConnections;
            return this;
        }

        public final Builder maxNonHeapMemory(long maxNonHeapMemoryBytes) {
            this.customConfig.put(OGlobalConfiguration.DIRECT_MEMORY_POOL_LIMIT, maxNonHeapMemoryBytes / pageSize);
            return this;
        }

        public final Builder cacheExpirationTime(Duration cacheExpirationTime) {
            this.cacheExpirationTime = cacheExpirationTime;
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
            this.postDecorator = this.postDecorator.andThen(QueryProvider.Decorator.of(decorators));
            return this;
        }

        public final Builder preDecorate(@Nonnull QueryProvider.Decorator... decorators) {
            this.preDecorator = QueryProvider.Decorator.of(decorators).andThen(this.preDecorator);
            return this;
        }

        private OrientDB createClient(String url, String serverUser, String serverPassword, String dbName, ODatabaseType dbType) {
            OrientDBConfig config = OrientDBConfig.builder()
                    .fromGlobalMap(customConfig)
                    .build();

            orientDbProvider = Optional.ofNullable(orientDbProvider)
                    .orElseGet(() -> DefaultOrientDbProvider.create(url, serverUser, serverPassword, config));

            OrientDB client = orientDbProvider.acquire();
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
            metricCollector = metricCollector.name("rxrepo.orientdb");
            MetricsQueryProviderDecorator decorator = MetricsQueryProviderDecorator.create(metricCollector);
            decorate(decorator);
            decorateExecutor(decorator.executorDecorator());
            sqlExecutorDecorator = sqlExecutorDecorator.andThen(MetricsSqlStatementExecutorDecorator.create(metricCollector));
            this.metricCollector = metricCollector;
            return this;
        }

        private SqlServiceFactory.Builder<?> serviceFactoryBuilder(OrientDbSessionProvider updateSessionProvider, OrientDbSessionProvider querySessionProvider) {
            Lazy<OrientDbReferencedObjectProvider> referencedObjectProviderLazy = Lazy.of(() -> OrientDbReferencedObjectProvider.create(querySessionProvider, cacheExpirationTime));
            Lazy<OrientDbStatementExecutor> statementExecutor = Lazy.of(() -> new OrientDbStatementExecutor(updateSessionProvider, querySessionProvider, referencedObjectProviderLazy.get()));

            return DefaultSqlServiceFactory.builder()
                    .metricCollector(metricCollector)
                    .schemaProvider(svc -> new OrientDbSqlSchemaGenerator(updateSessionProvider))
                    .statementExecutor(svc -> sqlExecutorDecorator.andThen(OrientDbMappingStatementExecutor.Decorator.create(svc.keyEncoder())).apply(statementExecutor.get()))
                    .expressionGenerator(svc -> OrientDbSqlExpressionGenerator.create(svc.keyEncoder()))
                    .dbNameProvider(Lazy.of(() -> querySessionProvider.session().map(ODatabaseDocument::getName).blockingGet()))
                    .statementProvider(svc -> new OrientDbSqlStatementProvider(
                            svc.expressionGenerator(),
                            svc.typeMapper(),
                            svc.keyEncoder(),
                            svc.dbNameProvider()))
                    .referenceResolver(svc -> new OrientDbSqlReferenceResolver(svc.statementProvider()))
                    .queryProviderGenerator(svc -> batchSupport ? OrientDbQueryProvider.create(svc, updateSessionProvider, cacheExpirationTime) : DefaultSqlQueryProvider.create(svc))
                    .keyEncoder(DigestKeyEncoder::create);
        }

        private OrientDbSessionProvider createSessionProvider(Lazy<OrientDB> dbClient, int maxConnections) {
            return OrientDbSessionProvider.create(() -> dbClient.get().open(dbName, user, password), maxConnections);
        }

        @Override
        protected SqlServiceFactory.Builder<?> serviceFactoryBuilder(RepositoryConfig config) {
            Lazy<OrientDB> dbClient = Lazy.of(() -> createClient(url, serverUser, serverPassword, dbName, dbType));
            OrientDbSessionProvider updateSessionProvider = createSessionProvider(dbClient, maxUpdateConnections);
            OrientDbSessionProvider querySessionProvider = createSessionProvider(dbClient, maxQueryConnections);
            ExecutorService queryResultPool = Executors.newWorkStealingPool(maxQueryConnections);

            return serviceFactoryBuilder(updateSessionProvider, querySessionProvider)
                    .onClose(Safe.ofRunnable(() -> {
                        queryResultPool.shutdown();
                        //noinspection ResultOfMethodCallIgnored
                        queryResultPool.awaitTermination(5, TimeUnit.SECONDS);
                    }), updateSessionProvider::close, querySessionProvider::close, () -> dbClient.ifExists(orientDbProvider::release))
                    .decorate(
                            preDecorator,
                            BatchUpdateQueryProviderDecorator.create(batchBufferSize),
                            RetryOnConcurrentConflictQueryProviderDecorator.create(Duration.ofMillis(config.retryInitialDurationMillis()), config.retryCount()),
                            OrientDbUpdateReferencesFirstQueryProviderDecorator.create(),
                            LiveQueryProviderDecorator.create(Duration.ofMillis(config.aggregationDebounceTimeMillis())),
                            ObserveOnSchedulingQueryProviderDecorator.create(Schedulers.from(executorDecorator.apply(queryResultPool))),
                            OrientDbDropDatabaseQueryProviderDecorator.create(dbClient, dbName),
                            SubscribeOnSchedulingQueryProviderDecorator.create(Schedulers.from(executorDecorator.apply(Runnable::run))),
                            postDecorator);
        }
    }
}
