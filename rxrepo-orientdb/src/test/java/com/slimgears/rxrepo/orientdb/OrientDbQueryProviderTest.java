package com.slimgears.rxrepo.orientdb;

import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.query.Repository;
import com.slimgears.rxrepo.query.decorator.SubscribeOnSchedulingQueryProviderDecorator;
import com.slimgears.rxrepo.test.*;
import com.slimgears.rxrepo.util.SchedulingProvider;
import com.slimgears.util.generic.MoreStrings;
import com.slimgears.util.stream.Streams;
import com.slimgears.util.test.logging.LogLevel;
import com.slimgears.util.test.logging.UseLogLevel;
import com.slimgears.util.test.logging.UseLogLevels;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.logging.LoggingMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.reactivex.Observable;
import io.reactivex.Scheduler;
import io.reactivex.observers.TestObserver;
import io.reactivex.schedulers.Schedulers;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@RunWith(Parameterized.class)
public class OrientDbQueryProviderTest extends AbstractRepositoryTest {
    private static final String dbUrl = "embedded:db";
    private static final String dbName = "{}_{}";
    private static LoggingMeterRegistry loggingMeterRegistry;

    @BeforeClass
    public static void setUpClass() {
        loggingMeterRegistry = new LoggingMeterRegistry();
        SimpleMeterRegistry simpleMeterRegistry = new SimpleMeterRegistry();
        Metrics.globalRegistry
                .add(loggingMeterRegistry)
                .add(simpleMeterRegistry);
    }

    @AfterClass
    public static void tearDownClass() {
        loggingMeterRegistry.stop();
        Metrics.globalRegistry.close();
    }

    @Parameterized.Parameter public OrientDbRepository.Type dbType;

    @Parameterized.Parameters
    public static OrientDbRepository.Type[] params() {
        return new OrientDbRepository.Type[] {
                OrientDbRepository.Type.Memory,
                OrientDbRepository.Type.Persistent};
    }

    @Override
    protected Repository createRepository(SchedulingProvider schedulingProvider) {
        String name = MoreStrings.format(dbName, dbType, testNameRule.getMethodName().replaceAll("\\[\\d+]", ""));
        Scheduler updateScheduler = Schedulers.from(Executors.newFixedThreadPool(5));
        Scheduler queryScheduler = Schedulers.from(Executors.newFixedThreadPool(5));
        return OrientDbRepository
                .builder()
                .url(dbUrl)
                .bufferDebounceTimeoutMillis(1000)
                .aggregationDebounceTimeMillis(2000)
                .type(dbType)
                .name(name)
                .schedulingProvider(schedulingProvider)
                .decorate(SubscribeOnSchedulingQueryProviderDecorator.create(updateScheduler, queryScheduler, Schedulers.from(Runnable::run)))
                .enableBatchSupport()
                .maxConnections(10)
                .build();
    }

    @Test @UseLogLevel(LogLevel.TRACE)
    public void testInsertThenUpdate() throws InterruptedException {
        super.testInsertThenUpdate();
    }

    @Test
    @UseLogLevel(LogLevel.TRACE)
    public void testRunQueriesFromMultipleThreads() throws InterruptedException {
        products.update(Products.createMany(1000)).blockingAwait();
        Observable.range(0, 10)
                .observeOn(Schedulers.newThread())
                .flatMap(i -> Observable.range(0, 100)
                        .map(j -> products.query().limit(1).retrieve()))
                .ignoreElements()
                .test()
                .await()
                .assertNoErrors();
    }

    @Test @Ignore
    //@UseLogLevel(LogLevel.TRACE)
    public void testRunUpdatesFromMultipleThreads() throws InterruptedException {
        Observable.range(0, 10)
                .observeOn(Schedulers.newThread())
                .flatMapCompletable(i -> Observable
                        .range(0, 100)
                        .flatMapCompletable(j -> products.update(Products.createMany(10))))
                .test()
                .await()
                .assertNoErrors();
    }

    @Test @Ignore
    @UseLogLevel(LogLevel.TRACE)
    public void testLiveQueriesFromMultipleThreads() throws InterruptedException {
        Observable.range(0, 10)
                .observeOn(Schedulers.newThread())
                .flatMap(i -> Observable
                        .range(0, 100)
                        .flatMap(j -> products.update(Products.createOne(i*100 + j)).ignoreElement().andThen(products.queryAndObserve())))
                .take(2000)
                .test()
                .await()
                .assertValueCount(2000)
                .assertNoErrors();
    }

    @Test
    @UseLogLevels({
            @UseLogLevel(logger = "com.slimgears.rxrepo.orientdb.OrientDbLiveQueryListener", value = LogLevel.TRACE),
            @UseLogLevel(LogLevel.INFO)})
    public void testAddProductThenUpdateInventoryInOrder() throws InterruptedException {
        super.testAddProductThenUpdateInventoryInOrder();
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCreateModifyOrder() throws InterruptedException {
        Inventory inventory = Inventory.create(UniqueId.inventoryId(1), "Inventory 1", null);
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
                .subscribe();

        inventories.update(inventory.toBuilder().name(inventory.name() + " - Updated").build())
                .subscribe();

        testObserver.await()
                .assertNoErrors();
    }
}
