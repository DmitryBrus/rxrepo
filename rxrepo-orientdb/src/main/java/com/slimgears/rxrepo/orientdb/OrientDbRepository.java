package com.slimgears.rxrepo.orientdb;

import com.google.common.collect.ImmutableMap;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.slimgears.nanometer.MetricCollector;
import com.slimgears.nanometer.Metrics;
import com.slimgears.rxrepo.query.Repository;
import com.slimgears.rxrepo.query.RepositoryConfig;
import com.slimgears.rxrepo.query.RepositoryConfigModelBuilder;
import com.slimgears.rxrepo.query.decorator.*;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.rxrepo.sql.DefaultSqlStatementProvider;
import com.slimgears.rxrepo.sql.SqlQueryProvider;
import com.slimgears.rxrepo.sql.SqlServiceFactory;
import com.slimgears.rxrepo.util.CachedRoundRobinSchedulingProvider;
import com.slimgears.rxrepo.util.SchedulingProvider;
import com.slimgears.rxrepo.util.SemaphoreLockProvider;
import com.slimgears.util.stream.Lazy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

public class OrientDbRepository {
    private final static Logger log = LoggerFactory.getLogger(OrientDbRepository.class);
    private final static MetricCollector metrics = Metrics.collector(OrientDbRepository.class);

    public enum Type {
        Memory,
        Persistent
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder implements RepositoryConfigModelBuilder<Builder> {
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
        private int batchBufferSize = 200;
        private int maxNotificationQueues = 10;
        private Duration maxQueueIdleTime = Duration.ofSeconds(120);
        private QueryProvider.Decorator decorator = QueryProvider.Decorator.identity();
        private Function<Executor, Executor> executorDecorator = Function.identity();
        private Function<SchedulingProvider, SchedulingProvider> schedulingProviderDecorator = Function.identity();
        private Supplier<SchedulingProvider> schedulingProvider = () -> CachedRoundRobinSchedulingProvider.create(maxNotificationQueues, maxQueueIdleTime, executorDecorator);
        private RepositoryConfig.Builder configBuilder = RepositoryConfig
                .builder()
                .retryCount(10)
                .retryInitialDurationMillis(10)
                .bufferDebounceTimeoutMillis(100)
                .aggregationDebounceTimeMillis(2000);

        private final Map<OGlobalConfiguration, Object> customConfig = new HashMap<>();

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

        public final Builder maxNotificationQueues(int maxNotificationQueues) {
            this.maxNotificationQueues = maxNotificationQueues;
            return this;
        }

        public final Builder maxQueueIdleTime(Duration duration) {
            this.maxQueueIdleTime = duration;
            return this;
        }

        public final Builder maxConnections(int maxConnections) {
            this.customConfig.put(OGlobalConfiguration.DB_POOL_MAX, maxConnections);
            return this;
        }

        public final Builder maxNonHeapMemory(int maxNonHeapMemoryBytes) {
            this.customConfig.put(OGlobalConfiguration.DIRECT_MEMORY_POOL_LIMIT, maxNonHeapMemoryBytes / pageSize);
            return this;
        }

        public final Builder schedulingProvider(SchedulingProvider schedulingProvider) {
            this.schedulingProvider = ()  -> schedulingProvider;
            return this;
        }

        public final Builder schedulingProvider(Function<SchedulingProvider, SchedulingProvider> schedulingProvider) {
            this.schedulingProviderDecorator = schedulingProvider;
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

        public final Repository build() {
            Objects.requireNonNull(url);
            Objects.requireNonNull(serverUser);
            Objects.requireNonNull(serverPassword);
            Objects.requireNonNull(dbName);
            Objects.requireNonNull(dbType);
            Objects.requireNonNull(user);
            Objects.requireNonNull(password);

            Lazy<OrientDB> dbClient = Lazy.of(() -> createClient(url, serverUser, serverPassword, dbName, dbType));

            AtomicInteger currentlyActiveSessions = new AtomicInteger();
            MetricCollector.Gauge activeSessionsGauge = metrics.gauge("activeSessions");

            OrientDbSessionProvider dbSessionProvider = OrientDbSessionProvider.create(
                    () -> {
                        ODatabaseSession dbSession = dbClient.get().open(dbName, user, password);
                        int newCount = currentlyActiveSessions.incrementAndGet();
                        log.debug("Created database connection (currently active connections: {})", newCount);
                        activeSessionsGauge.record(newCount);
                        return dbSession;
                    },
                    dbSession -> {
                        int newCount = currentlyActiveSessions.decrementAndGet();
                        log.debug("Database connection closed (currently active connections: {}", newCount);
                        activeSessionsGauge.record(newCount);
                    });

            RepositoryConfig config = configBuilder.build();
            return serviceFactoryBuilder(dbSessionProvider)
                    .decorate(
                            LockQueryProviderDecorator.create(SemaphoreLockProvider.create()),
                            LiveQueryProviderDecorator.create(Duration.ofMillis(config.aggregationDebounceTimeMillis())),
                            ObserveOnSchedulingQueryProviderDecorator.create(schedulingProvider.get()),
                            batchSupport ? OrientDbUpdateReferencesFirstQueryProviderDecorator.create() : UpdateReferencesFirstQueryProviderDecorator.create(),
                            OrientDbDropDatabaseQueryProviderDecorator.create(dbClient, dbName),
                            decorator)
                    .buildRepository(config)
                    .onClose(repo -> dbClient.close());
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

        private SqlServiceFactory.Builder serviceFactoryBuilder(OrientDbSessionProvider dbSessionProvider) {
            return SqlServiceFactory.builder()
                    .schemaProvider(svc -> new OrientDbSchemaProvider(dbSessionProvider))
                    .statementExecutor(svc -> OrientDbMappingStatementExecutor.decorate(new OrientDbStatementExecutor(dbSessionProvider)))
                    .expressionGenerator(OrientDbSqlExpressionGenerator::new)
                    .assignmentGenerator(svc -> new OrientDbAssignmentGenerator(svc.expressionGenerator()))
                    .statementProvider(svc -> new DefaultSqlStatementProvider(svc.expressionGenerator(), svc.assignmentGenerator(), svc.schemaProvider()))
                    .referenceResolver(svc -> new OrientDbReferenceResolver(svc.statementProvider()))
                    .queryProviderGenerator(svc -> batchSupport ? OrientDbQueryProvider.create(svc, dbSessionProvider, batchBufferSize) : SqlQueryProvider.create(svc))
                    .schedulingProvider(() -> schedulingProviderDecorator.apply(schedulingProvider.get()));
        }
    }
}
