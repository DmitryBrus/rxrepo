package com.slimgears.rxrepo.jdbc;

import com.google.common.base.Stopwatch;
import com.slimgears.rxrepo.postgres.PostgresRepository;
import com.slimgears.rxrepo.query.Repository;
import com.slimgears.rxrepo.sql.AbstractSqlStatementExecutorDecorator;
import com.slimgears.rxrepo.sql.SqlStatement;
import com.slimgears.rxrepo.sql.SqlStatementExecutor;
import com.slimgears.rxrepo.test.AbstractRepositoryTest;
import com.slimgears.rxrepo.util.PropertyResolver;
import com.slimgears.util.junit.DockerRules;
import com.slimgears.util.test.logging.LogLevel;
import com.slimgears.util.test.logging.UseLogLevel;
import io.reactivex.Completable;
import io.reactivex.Observable;
import io.reactivex.Single;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Ignore
public class PostgresRepositoryTest  extends AbstractRepositoryTest {
    private AtomicLong totalMillis;
    private AtomicLong totalCount;

    @ClassRule
    public static TestRule postgresRule = PostgresTestUtils.rule();

    static class StopWatchDecorator extends AbstractSqlStatementExecutorDecorator {
        private final AtomicLong totalMillis;
        private final AtomicLong totalCount;

        protected StopWatchDecorator(SqlStatementExecutor underlyingExecutor,
                                     AtomicLong totalMillis,
                                     AtomicLong totalCount) {
            super(underlyingExecutor);
            this.totalMillis = totalMillis;
            this.totalCount = totalCount;
        }

        public static SqlStatementExecutor.Decorator create(AtomicLong totalMillis, AtomicLong totalCount) {
            return src -> new StopWatchDecorator(src, totalMillis, totalCount);
        }

        @Override
        public Observable<PropertyResolver> executeQuery(SqlStatement statement) {
            Stopwatch stopwatch = Stopwatch.createUnstarted();
            return super.executeQuery(statement)
                    .doOnSubscribe(d -> stopwatch.start())
                    .doFinally(() -> {
                        totalMillis.addAndGet(stopwatch.elapsed(TimeUnit.MILLISECONDS));
                        totalCount.incrementAndGet();
                    });
        }

        @Override
        public Observable<PropertyResolver> executeCommandReturnEntries(SqlStatement statement) {
            Stopwatch stopwatch = Stopwatch.createUnstarted();
            return super.executeCommandReturnEntries(statement)
                    .doOnSubscribe(d -> stopwatch.start())
                    .doFinally(() -> {
                        totalMillis.addAndGet(stopwatch.elapsed(TimeUnit.MILLISECONDS));
                        totalCount.incrementAndGet();
                    });
        }

        @Override
        public Single<Integer> executeCommandReturnCount(SqlStatement statement) {
            Stopwatch stopwatch = Stopwatch.createUnstarted();
            return super.executeCommandReturnCount(statement)
                    .doOnSubscribe(d -> stopwatch.start())
                    .doFinally(() -> {
                        totalMillis.addAndGet(stopwatch.elapsed(TimeUnit.MILLISECONDS));
                        totalCount.incrementAndGet();
                    });
        }

        @Override
        public Completable executeCommands(Iterable<SqlStatement> statements) {
            Stopwatch stopwatch = Stopwatch.createUnstarted();
            return super.executeCommands(statements)
                    .doOnSubscribe(d -> stopwatch.start())
                    .doFinally(() -> {
                        totalMillis.addAndGet(stopwatch.elapsed(TimeUnit.MILLISECONDS));
                        totalCount.incrementAndGet();
                    });
        }

        @Override
        public Completable executeCommand(SqlStatement statement) {
            Stopwatch stopwatch = Stopwatch.createUnstarted();
            return super.executeCommand(statement)
                    .doOnSubscribe(d -> stopwatch.start())
                    .doFinally(() -> {
                        totalMillis.addAndGet(stopwatch.elapsed(TimeUnit.MILLISECONDS));
                        totalCount.incrementAndGet();
                    });
        }
    }

    @Override
    public void setUp() throws Exception {
        totalMillis = new AtomicLong();
        totalCount = new AtomicLong();
        super.setUp();
    }

    @Override
    public void tearDown() {
        System.out.println(
                "------ Total execution time: " + Duration.ofMillis(totalMillis.get()) +
                ", count: " + totalCount +
                ", average time: " + Duration.ofMillis(totalMillis.get() / totalCount.get()));
        super.tearDown();
    }

    @Test
    @UseLogLevel(LogLevel.TRACE)
    public void testInsertThenUpdate() throws InterruptedException {
        super.testInsertThenUpdate();
    }

    @Override
    protected Repository createRepository() {
        return PostgresRepository
                .builder()
                .connection(PostgresTestUtils.connectionUrl)
                .schemaName(PostgresTestUtils.schemaName)
                .decorateExecutor(StopWatchDecorator.create(totalMillis, totalCount))
                .enableBatch(1000)
                .build();
    }
}
