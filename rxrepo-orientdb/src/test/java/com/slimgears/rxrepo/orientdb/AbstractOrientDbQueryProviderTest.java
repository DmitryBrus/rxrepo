package com.slimgears.rxrepo.orientdb;

import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.query.Repository;
import com.slimgears.rxrepo.query.decorator.OperationTimeoutQueryProviderDecorator;
import com.slimgears.rxrepo.test.*;
import com.slimgears.util.generic.MoreStrings;
import com.slimgears.util.stream.Streams;
import com.slimgears.util.test.logging.LogLevel;
import com.slimgears.util.test.logging.UseLogLevel;
import com.slimgears.util.test.logging.UseLogLevels;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.logging.LoggingMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.reactivex.Observable;
import io.reactivex.observers.BaseTestConsumer;
import io.reactivex.observers.TestObserver;
import io.reactivex.schedulers.Schedulers;
import org.apache.commons.io.FileUtils;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public abstract class AbstractOrientDbQueryProviderTest extends AbstractRepositoryTest {
    private static final String dbName = "{}_{}";
    private static LoggingMeterRegistry loggingMeterRegistry;

    @BeforeClass
    public static void setUpClass() throws IOException {
        FileUtils.deleteDirectory(new File("db"));
        loggingMeterRegistry = new LoggingMeterRegistry();
        SimpleMeterRegistry simpleMeterRegistry = new SimpleMeterRegistry();
        Metrics.globalRegistry
                .add(loggingMeterRegistry)
                .add(simpleMeterRegistry);
    }

    @AfterClass
    public static void tearDownClass() {
        if (loggingMeterRegistry != null) {
            loggingMeterRegistry.stop();
            Metrics.globalRegistry.close();
        }
    }

    protected Repository createRepository(String dbUrl, OrientDbRepository.Type dbType) {
        String name = MoreStrings.format(dbName, dbType, testNameRule.getMethodName().replaceAll("\\[\\d+]", ""));
        return OrientDbRepository
                .builder()
                .url(dbUrl)
                .bufferDebounceTimeoutMillis(1000)
                .aggregationDebounceTimeMillis(2000)
                .type(dbType)
                .name(name)
                .decorate(
                        OperationTimeoutQueryProviderDecorator.create(Duration.ofSeconds(20), Duration.ofSeconds(360)))
                .enableBatchSupport(1000)
                .maxConnections(10)
                .build();
    }

    @Test
    @UseLogLevel(LogLevel.TRACE)
    public void testInsertThenUpdate() throws InterruptedException {
        super.testInsertThenUpdate();
    }

    @Test
    @UseLogLevel(LogLevel.TRACE)
    public void testRunQueriesFromMultipleThreads() throws InterruptedException {
        products.update(Products.createMany(1000)).blockingAwait();
        Observable.range(0, 10)
                .flatMap(i -> Observable.range(0, 100)
                        .map(j -> products.query().limit(1).retrieve())
                        .subscribeOn(Schedulers.computation()))
                .ignoreElements()
                .test()
                .await()
                .assertNoErrors();
    }

    @Test @Ignore
    //@UseLogLevel(LogLevel.TRACE)
    public void testRunUpdatesFromMultipleThreads() throws InterruptedException {
        Observable.range(0, 10)
                .flatMapCompletable(i -> Observable
                        .range(0, 100)
                        .flatMapCompletable(j -> products.update(Products.createMany(10)))
                        .subscribeOn(Schedulers.computation()))
                .test()
                .await()
                .assertNoErrors();
    }

    @Test
    @UseLogLevel(LogLevel.INFO)
    public void testLiveQueriesFromMultipleThreads() {
        Observable.range(0, 10)
                .flatMap(i -> products
                        .update(Products.createMany(i*10, 10))
                        .andThen(products.queryAndObserve())
                        .subscribeOn(Schedulers.computation()))
                .take(200)
                .test()
                .assertOf(TestUtils.countAtLeast(200, Duration.ofSeconds(60)));
    }

    @Test
    @UseLogLevels({
            @UseLogLevel(logger = "com.slimgears.rxrepo.orientdb.OrientDbLiveQueryListener", value = LogLevel.TRACE),
            @UseLogLevel(LogLevel.INFO)})
    @Ignore
    public void testAddProductThenUpdateInventoryInOrder() throws InterruptedException {
        super.testAddProductThenUpdateInventoryInOrder();
    }

    @SuppressWarnings("unchecked")
    @Test
    @UseLogLevel(LogLevel.INFO)
    public void testCreateModifyOrder() throws InterruptedException {
        Inventory inventory = Inventory.builder()
                        .id(UniqueId.inventoryId(1))
                        .name("Inventory 1")
                        .build();
        inventories.update(inventory)
                .ignoreElement()
                .blockingAwait();

        final int productCount = 1000;

        AtomicLong createdCount = new AtomicLong();

        TestObserver<Notification<Product>> testObserver = products.observe(Product.$.name, Product.$.inventory.name)
                .doOnNext(n -> {
                    if (n.isCreate()) {
                        createdCount.incrementAndGet();
                    }
                    if (n.isModify()) {
                        Assert.assertEquals(productCount, createdCount.get());
                    }
                })
                .take(productCount*2)
                .test()
                .assertSubscribed();

        products.update(Streams.fromIterable(Products.createMany(productCount))
                .map(p -> p.toBuilder().inventory(inventory).build())
                .collect(Collectors.toList()))
                .blockingAwait();

        inventories.update(inventory.toBuilder().name(inventory.name() + " - Updated").build())
                .ignoreElement()
                .blockingAwait();

        testObserver.await()
                .assertNoErrors();
    }

    @Ignore @Test @UseLogLevel(LogLevel.TRACE)
    public void testDecoratorEmptyEntitySet() {
        products
                .query()
                .queryAndObserve()
                .test()
                .awaitCount(1, BaseTestConsumer.TestWaitStrategy.SLEEP_100MS, 30000)
                .assertNoErrors()
                .assertValueCount(0);

        products
                .query()
                .observeCount()
                .test()
                .awaitCount(2, BaseTestConsumer.TestWaitStrategy.SLEEP_100MS, 30000)
                .assertNoErrors()
                .assertValueCount(1)
                .assertValueAt(0, 0L);

        products
                .query()
                .liveSelect()
                .observe()
                .test()
                .awaitCount(1, BaseTestConsumer.TestWaitStrategy.SLEEP_100MS, 30000)
                .assertNoErrors()
                .assertValueCount(0);
    }
}
