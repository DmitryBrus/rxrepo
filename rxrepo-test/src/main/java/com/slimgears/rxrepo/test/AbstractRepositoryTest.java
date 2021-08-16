package com.slimgears.rxrepo.test;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.slimgears.rxrepo.expressions.Aggregator;
import com.slimgears.rxrepo.expressions.ObjectExpression;
import com.slimgears.rxrepo.query.*;
import com.slimgears.rxrepo.query.provider.QueryInfo;
import com.slimgears.util.generic.MoreStrings;
import com.slimgears.util.junit.AnnotationRulesJUnit;
import com.slimgears.util.stream.Streams;
import com.slimgears.util.test.logging.LogLevel;
import com.slimgears.util.test.logging.UseLogLevel;
import io.reactivex.Maybe;
import io.reactivex.Observable;
import io.reactivex.ObservableTransformer;
import io.reactivex.observers.TestObserver;
import io.reactivex.subjects.CompletableSubject;
import org.junit.*;
import org.junit.rules.MethodRule;
import org.junit.rules.TestName;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.slimgears.rxrepo.test.TestUtils.*;
import static java.util.Objects.requireNonNull;

@SuppressWarnings("unchecked")
public abstract class AbstractRepositoryTest {
    private final Logger log = LoggerFactory.getLogger(getClass());

    @Rule public final TestName testNameRule = new TestName();
    @Rule public final MethodRule annotationRules = AnnotationRulesJUnit.rule();
    @Rule public final Timeout timeout = new Timeout(4, TimeUnit.MINUTES);
    @Rule public final TestRule memoryMeter = new MemoryUsageRule("Test");
    @ClassRule public static final TestRule classMemoryMeter = new MemoryUsageRule("Class");

    private Repository repository;
    protected EntitySet<UniqueId, Product> products;
    protected EntitySet<UniqueId, Inventory> inventories;

    @Before
    public void setUp() throws Exception {
        this.repository = createRepository();
        this.products = repository.entities(Product.metaClass);
        this.inventories = repository.entities(Inventory.metaClass);
        System.out.println("Starting test: " + testNameRule.getMethodName());
    }

    @After
    public void tearDown() {
        System.out.println("Test finished: " + testNameRule.getMethodName());
        if (System.getProperty("test.noClear") == null) {
            this.repository.clear().doOnComplete(this.repository::close)
                    .onErrorComplete()
                    .blockingAwait(10000, TimeUnit.MILLISECONDS);
        } else {
            this.repository.close();
        }
    }

    protected abstract Repository createRepository();

    @Test
    @Ignore
    public void testLiveSelectThenInsert() throws InterruptedException {
        EntitySet<UniqueId, Product> productSet = repository.entities(Product.metaClass);

        AtomicInteger counter = new AtomicInteger();

        TestObserver<Notification<Product>> productUpdatesTest = productSet
                .query()
                //.where(Product.$.price.greaterThan(110))
                .liveSelect()
                .queryAndObserve()
                .doOnNext(n -> System.out.println("Received notifications: " + counter.incrementAndGet()))
                .doOnSubscribe(d -> System.out.println("Subscribed for live query"))
                .test()
                .assertValueCount(0);

        TestObserver<Long> productCount = productSet
                .query()
                .liveSelect()
                .count()
                .doOnNext(c -> System.out.println("Count: " + c))
                .test()
                .assertValueCount(0);

        productSet.update(Products.createMany(2000))
                .test()
                .await()
                .assertNoErrors();

        productUpdatesTest
                .assertOf(countAtLeast(2000, Duration.ofSeconds(40)));

        productSet.delete().where(Product.$.key.id.betweenExclusive(1000, 1300))
                .execute()
                .test()
                .await()
                .assertNoErrors()
                .assertValue(299);

        productUpdatesTest
                .assertOf(countAtLeast(2200, Duration.ofSeconds(40)))
                .assertValueAt(2199, Notification::isDelete)
                .assertNoErrors();

        productCount
                .assertNoErrors();
    }

    @Test
    @UseLogLevel(LogLevel.TRACE)
    public void testAddAndRetrieveByKey() throws InterruptedException {
        EntitySet<UniqueId, Product> productSet = repository.entities(Product.metaClass);
        Product product = Product.builder()
                .name("Product-1")
                .key(UniqueId.productId(1))
                .price(1001)
                .build();

        productSet.update(product).test().await().assertNoErrors();
        Assert.assertEquals(Long.valueOf(1), productSet.query().where(Product.$.key.eq(UniqueId.productId(1))).count().blockingGet());
    }

    @Test
    @UseLogLevel(LogLevel.TRACE)
    public void testUpdateReferencedEntity() throws InterruptedException {
        EntitySet<UniqueId, Product> productSet = repository.entities(Product.metaClass);
        Product product = Product.builder()
                .name("Product-1")
                .key(UniqueId.productId(1))
                .price(1001)
                .build();

        productSet.update(product).test().await().assertNoErrors();
        product = productSet.query().where(Product.$.key.eq(UniqueId.productId(1))).first().blockingGet();
        Assert.assertNull(product.inventory());

        Inventory inventory = Inventory.builder()
                .id(UniqueId.inventoryId(1))
                .name("Inventory-1")
                .build();

        Product updatedProduct = product.toBuilder().inventory(inventory).build();

        productSet.update(updatedProduct).test().await().assertNoErrors();

        product = productSet.query()
                .where(Product.$.key.eq(UniqueId.productId(1)))
                .select()
                .properties(Product.$.inventory)
                .first().blockingGet();
        Assert.assertNotNull(product.inventory());
    }

    @Test
    public void testAddAlreadyExistingObject() throws InterruptedException {
        EntitySet<UniqueId, Product> productSet = repository.entities(Product.metaClass);
        Product product = Product.builder()
                .name("Product-1")
                .key(UniqueId.productId(1))
                .price(1001)
                .build();

        productSet.update(product).test().await().assertNoErrors();
        productSet.update(product).test().await().assertNoErrors();
        Assert.assertEquals(Long.valueOf(1), productSet.query().count().blockingGet());
    }

    @Test
    public void testAddRecursiveInventory() throws InterruptedException {
        Inventory inventory = Inventory.builder()
                .id(UniqueId.inventoryId(1))
                .name("Inventory-1")
                .inventory(Inventory.builder()
                        .id(UniqueId.inventoryId(2))
                        .name("Inventory-2")
                        .build())
                .build();

        EntitySet<UniqueId, Inventory> inventorySet = repository.entities(Inventory.metaClass);
        inventorySet
                .update(inventory)
                .test()
                .await()
                .assertNoErrors();

        inventorySet.findAll()
                .sorted(Comparator.comparing(c -> c.id().id()))
                .test()
                .assertOf(countAtLeast(2))
                .assertValueAt(0, i -> Objects.equals(i.id(), inventory.id()));
    }

    @Test
    public void testAddAndRetrieveSingleEntity() {
        Product product = Products.createOne();
        EntitySet<UniqueId, Product> productSet = repository.entities(Product.metaClass);
        productSet.update(product)
                .ignoreElement()
                .blockingAwait();

        productSet.findAll()
                .test()
                .assertOf(countAtLeast(1))
                .assertValue(p -> Objects.equals(p.key(), product.key()));
    }

    @Test
    public void testAddSameInventory() throws InterruptedException {
        EntitySet<UniqueId, Product> productSet = repository.entities(Product.metaClass);
        EntitySet<UniqueId, Inventory> inventorySet = repository.entities(Inventory.metaClass);
        List<Product> products = Arrays.asList(
                Product.builder()
                        .name("Product-1")
                        .key(UniqueId.productId(1))
                        .price(1001)
                        .inventory(Inventory
                                .builder()
                                .id(UniqueId.inventoryId(2))
                                .name("Inventory-2")
                                .build())
                        .build(),
                Product.builder()
                        .name("Product-2")
                        .key(UniqueId.productId(2))
                        .price(1002)
                        .inventory(Inventory
                                .builder()
                                .id(UniqueId.inventoryId(2))
                                .name("Inventory-2")
                                .build())
                        .build());

        productSet
                .update(products)
                .test()
                .await()
                .assertNoErrors()
                .assertComplete()
//                .assertValueCount(1)
//                .assertValueAt(0, items -> items.size() == 2)
//                .assertValueAt(0, items -> items.stream().anyMatch(p -> Objects.equals(p.name(), "Product-1")))
//                .assertValueAt(0, items -> items.stream().anyMatch(p -> Objects.equals(p.name(), "Product-2")));
        ;

        Assert.assertEquals(Long.valueOf(1), inventorySet.query().count().blockingGet());
    }

    @Test
    public void testInsertThenLiveSelectShouldReturnAdded() throws InterruptedException {
        EntitySet<UniqueId, Product> productSet = repository.entities(Product.metaClass);
        Iterable<Product> products = Products.createMany(200);
        productSet.update(products).test().await();

        productSet.query()
                .liveSelect()
                .queryAndObserve()
                .doOnNext(System.out::println)
                .test()
                .assertOf(countAtLeast(200))
                .assertValueAt(10, NotificationPrototype::isCreate);
    }

    @Test @UseLogLevel(LogLevel.TRACE)
    public void testSearchTextWithSpecialChars() {
        products.update(Products.createOne().toBuilder()
                .key(UniqueId.productId(1))
                .name("begin :> Product / {with} (special) % [chars]; - and more\\ <: end").build())
                .ignoreElement()
                .blockingAwait();

        // Sanity check
        Assert.assertEquals(Long.valueOf(0), products.findAll(Product.$.searchText("Product Foo")).count().blockingGet());

        Assert.assertEquals(Long.valueOf(1), products.findAll(Product.$.searchText(":> Product / {with} (special) % [chars]; - and more\\")).count().blockingGet());
    }

    @Test
    @UseLogLevel(LogLevel.TRACE)
    public void testInsertThenLiveSelectCountShouldReturnCount() throws InterruptedException {
        EntitySet<UniqueId, Product> productSet = repository.entities(Product.metaClass);
        Iterable<Product> products = Products.createMany(200);
        productSet.update(products).test().await();

        TestObserver<Long> countObserver = productSet.query()
                .liveSelect()
                .count()
                .takeUntil(c -> c == 89)
                .test();

        productSet.delete()
                .where(Product.$.searchText("Product-1"))
                .execute()
                .test()
                .await()
                .assertValue(111);

        countObserver
                .await()
                .assertComplete();
    }

    @Test
    @UseLogLevel(LogLevel.TRACE)
    public void testAtomicUpdate() throws InterruptedException {
        EntitySet<UniqueId, Product> productSet = repository.entities(Product.metaClass);
        CompletableSubject trigger1 = CompletableSubject.create();
        CompletableSubject trigger2 = CompletableSubject.create();

        productSet.update(Products.createMany(1)).test().await().assertNoErrors();

        TestObserver<Supplier<Product>> prodUpdateTester1 = productSet
                .update(UniqueId.productId(0), prod -> prod
                        .flatMap(p -> Maybe.just(p.toBuilder().name(p.name() + " Updated name #1").build())
                                .delay(trigger1.toFlowable())))
                .test();

        Thread.sleep(500);

        TestObserver<Supplier<Product>> prodUpdateTester2 = productSet
                .update(UniqueId.productId(0), prod -> prod
                        .flatMap(p -> Maybe.just(p.toBuilder().name(p.name() + " Updated name #2").build())
                                .delay(trigger2.toFlowable())))
                .test();

        Thread.sleep(500);
        trigger1.onComplete();
        prodUpdateTester1.await().assertNoErrors();

        trigger2.onComplete();
        prodUpdateTester2
                .await()
                .assertNoErrors()
                .assertValue(p -> "Product-0 Updated name #1 Updated name #2".equals(p.get().name()));
    }

    @Test
    //@UseLogLevel(UseLogLevel.Level.FINEST)
    public void testFilteredAtomicUpdate() throws InterruptedException {
        EntitySet<UniqueId, Product> productSet = repository.entities(Product.metaClass);

        productSet.update(Products.createMany(1)).test().await().assertNoErrors();
        productSet
                .update(UniqueId.productId(0), prod -> prod
                        .flatMap(p -> Maybe.just(p.toBuilder().name("Product " + p.key().id() + " Updated name").build())))
                .test()
                .await()
                .assertComplete()
                .assertValueCount(1);

        productSet
                .update(UniqueId.productId(0), prod -> prod
                        .flatMap(p -> Maybe.just(p.toBuilder().name("Product " + p.key().id() + " Updated name").build())))
                .test()
                .await()
                .assertComplete()
                .assertValueCount(1);
    }

    @Test
    public void testInsertThenRetrieve() throws InterruptedException {
        EntitySet<UniqueId, Product> productSet = repository.entities(Product.metaClass);
        Iterable<Product> products = Products.createMany(200);
        Stopwatch stopwatch = Stopwatch.createUnstarted();
        productSet
                .update(products)
                .doOnSubscribe(d -> stopwatch.start())
                .doFinally(stopwatch::stop)
                .test()
                .await()
                .assertNoErrors();

        System.out.println("Duration: " + stopwatch.elapsed(TimeUnit.MILLISECONDS) + " ms");
        productSet.query()
                .where(Product.$.name.contains("21"))
                .select()
                .count()
                .test()
                .await()
                .assertValue(2L);

        productSet.query()
                .where(Product.$.name.contains("21"))
                .select(Product.$.price)
                .aggregate(Aggregator.sum())
                .test()
                .await()
                .assertValue(212);

        productSet
                .query()
                .where(Product.$.name.contains("131"))
                .select()
                .retrieve(Product.$.key, Product.$.price, Product.$.inventory.id, Product.$.inventory.name)
                .test()
                .await()
                .assertNoErrors()
                .assertValue(p -> p.name() == null)
                .assertValue(p -> p.key().id() == 131)
                .assertValue(p -> p.price() == 151)
                .assertValue(p -> "Inventory-11".equals(requireNonNull(p.inventory()).name()))
                .assertValueCount(1);

        productSet
                .query()
                .where(Product.$.type.in(ProductEntity.Type.ComputerSoftware, ProductEntity.Type.ComputeHardware))
                .skip(20)
                .retrieve()
                .test()
                .await()
                .assertValueCount(113);
    }

    @Test
    @UseLogLevel(LogLevel.TRACE)
    public void testInsertThenSearch() throws InterruptedException {
        EntitySet<UniqueId, Product> productSet = repository.entities(Product.metaClass);
        Iterable<Product> products = Products.createMany(100);
        Stopwatch stopwatch = Stopwatch.createUnstarted();
        productSet
                .update(products)
                .doOnSubscribe(d -> stopwatch.start())
                .doFinally(stopwatch::stop)
                .test()
                .await()
                .assertNoErrors();

        productSet
                .query()
                .where(Product.$.searchText("Product-31 ComputeHardware"))
                .select()
                .retrieve(Product.$.key, Product.$.name, Product.$.price, Product.$.type, Product.$.inventory.id, Product.$.inventory.name)
                .doOnNext(System.out::println)
                .test()
                .await()
                .assertNoErrors()
                .assertValueCount(1);
    }

    @Test
    public void testQueryWithEmptyMapping() {
        EntitySet<UniqueId, Product> productSet = repository.entities(Product.metaClass);
        Iterable<Product> products = Products.createMany(10);
        productSet.update(products).blockingAwait();
        productSet.query()
                .where(Product.$.name.startsWith("Product"))
                .select(ObjectExpression.arg(Product.class))
                .retrieve()
                .test()
                .assertOf(countExactly(10));

        productSet.query()
                .where(Product.$.name.startsWith("Product"))
                .limit(1)
                .observeAsList()
                .take(1)
                .test()
                .assertOf(countExactly(1))
                .assertValue(l -> l.size() == 1);
    }

    @Test
    @Ignore
    public void testInsertThenUpdate() throws InterruptedException {
        EntitySet<UniqueId, Product> productSet = repository.entities(Product.metaClass);
        Iterable<Product> products = Products.createMany(200);
        productSet
                .update(products)
                .test()
                .await()
                .assertNoErrors();

        productSet
                .update()
                .set(Product.$.name, Product.$.name.concat(" - ").concat(Product.$.inventory.name.asString()))
                .where(Product.$.key.id.betweenExclusive(100, 150))
                .limit(20)
                .execute()
                .test()
                .await()
                .assertNoErrors()
                .assertNoTimeout()
                .assertValue(20);

        productSet
                .query()
                .where(Product.$.key.id.betweenExclusive(100, 150))
                .limit(20)
                .retrieve(Product.$.name, Product.$.inventory)
                .test()
                .awaitCount(20)
                .assertNoErrors()
                .assertNoTimeout()
                .assertValueAt(15, pr -> {
                    System.out.println(pr);
                    Matcher matcher = Pattern.compile("Product-([0-9]+) - Inventory-([0-9]+)").matcher(requireNonNull(pr.name()));
                    return matcher.matches() &&
                            Integer.parseInt(matcher.group(1)) == pr.key().id() &&
                            Integer.parseInt(matcher.group(2)) == requireNonNull(pr.inventory()).id().id();
                });
    }

    @Test
    public void testObserveReferencedObjectProperties() {
        repository.entities(Product.metaClass)
                .update(Products.createOne().toBuilder().inventory(null).build())
                .ignoreElement()
                .blockingAwait();

        repository.entities(Product.metaClass)
                .query()
                .liveSelect()
                .properties(Product.$.inventory.name)
                .observeAs(Notifications.toList())
                .test()
                .awaitCount(1)
                .assertValue(l -> l.size() == 1)
                .assertValue(l -> l.get(0).inventory() == null);
    }

    @Test
    @UseLogLevel(LogLevel.TRACE)
    public void testObserveCountThenDelete() throws InterruptedException {
        repository.entities(Product.metaClass)
                .update(Products.createMany(10))
                .blockingAwait();

        TestObserver<Long> count = repository.entities(Product.metaClass)
                .query()
                .where(Product.$.price.greaterOrEqual(100))
                .observeCount()
                .takeUntil(c -> c == 0)
                .test()
                .assertSubscribed();

        count.awaitCount(1)
                .assertValueCount(1)
                .assertValue(10L);

        repository.entities(Product.metaClass).deleteAll(Product.$.price.greaterOrEqual(100))
                .blockingAwait();

        count.await().assertComplete();
    }

    @Test
    @UseLogLevel(LogLevel.TRACE)
    public void testPartialRetrieve() throws InterruptedException {
        EntitySet<UniqueId, Product> productSet = repository.entities(Product.metaClass);
        Iterable<Product> products = Products.createMany(10);
        productSet
                .update(products)
                .test()
                .await()
                .assertNoErrors();

        productSet
                .query()
                .where(Product.$.price.lessOrEqual(115))
                .orderBy(Product.$.key.id)
                .orderBy(Product.$.productionDate)
                .retrieve(Product.$.name)
                .doOnNext(System.out::println)
                .test()
                .await()
                .assertNoErrors()
                .assertValueAt(1, p -> "Product-1".equals(p.name()));
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testEntityWithListOfReferenceField() throws InterruptedException {
        EntitySet<UniqueId, Storage> storages = repository.entities(Storage.metaClass);
        storages.update(Storage.builder()
                .key(UniqueId.storageId(1))
                .productList(ImmutableList.copyOf(Products.createMany(10)))
                .build())
                .test()
                .await()
                .assertNoErrors();

        storages
                .query()
                .select()
                .properties(Storage.$.productList)
                .retrieve()
                .test()
                .await()
                .assertNoErrors()
                .assertValueCount(1)
                .assertValue(s -> s.productList().size() == 10);
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testEntityWithStringByReferenceMapField() throws InterruptedException {
        EntitySet<UniqueId, Storage> storages = repository.entities(Storage.metaClass);
        storages.update(Storage.builder()
                .key(UniqueId.storageId(1))
                .productMapByName(Streams
                        .fromIterable(Products.createMany(10))
                        .collect(ImmutableMap.toImmutableMap(Product::name, p -> p)))
                .build())
                .test()
                .await()
                .assertNoErrors();

        storages.query()
                .retrieve(Storage.$.productMapByName)
                .test()
                .await()
                .assertNoErrors()
                .assertValueCount(1)
                .assertValue(s -> s.productMapByName().size() == 10);
    }

    @Test
    public void testEntityWithListOfStringField() throws InterruptedException {
        Product product = Product.builder()
                .key(UniqueId.productId(1))
                .name("Product1")
                .inventory(Inventory
                        .builder()
                        .id(UniqueId.inventoryId(2))
                        .name("Inventory2")
                        .build())
                .price(100)
                .type(ProductEntity.Type.ComputeHardware)
                .aliases(ImmutableList.of("p1", "p2"))
                .build();

        EntitySet<UniqueId, Product> products = repository.entities(Product.metaClass);
        products
                .update(product)
                .test()
                .await()
                .assertNoErrors();

        products
                .query()
                .retrieve(Product.$.aliases)
                .test()
                .await()
                .assertValue(p -> requireNonNull(p.aliases()).size() == 2);
    }

    @Test
    public void testFilterByNestedCompoundKey() throws InterruptedException {
        repository.entities(Product.metaClass).update(Products.createMany(10)).test().await();
        repository.entities(Product.metaClass).query()
                .where(Product.$.inventory.id.eq(UniqueId.inventoryId(0)))
                .retrieve()
                .test()
                .await()
                .assertValueCount(10);
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testEntityWithListOfEmbeddedField() throws InterruptedException {
        Product product = Product.builder()
                .key(UniqueId.productId(1))
                .name("Product1")
                .inventory(Inventory
                        .builder()
                        .id(UniqueId.inventoryId(2))
                        .name("Inventory2")
                        .build())
                .price(100)
                .type(ProductEntity.Type.ComputeHardware)
                .relatedIds(ImmutableList.of(UniqueId.storageId(3), UniqueId.inventoryId(2), UniqueId.productId(1)))
                .build();

        repository.entities(Product.metaClass)
                .update(product)
                .test()
                .await()
                .assertNoErrors();

        repository.entities(Product.metaClass)
                .query()
                .select()
                .retrieve(Product.$.relatedIds)
                .take(1)
                .singleElement()
                .test()
                .await()
                .assertValue(p -> p.relatedIds().size() == 3)
                .assertValue(p -> p.relatedIds().get(0).id() == 3);
    }

    @Test
    public void testInsertThenQueryValueIn() throws InterruptedException {
        repository.entities(Product.metaClass)
                .update(Products.createMany(10))
                .test()
                .await();

        repository.entities(Product.metaClass)
                .query()
                .where(Product.$.key.in(UniqueId.productId(2), UniqueId.productId(3)))
                .retrieve()
                .test()
                .await()
                .assertNoErrors()
                .assertValueCount(2);
    }

    @Test
    public void testFilteredLiveQuery() {
        EntitySet<UniqueId, Product> products = repository.entities(Product.metaClass);

        products.update(Product
                .builder()
                .key(UniqueId.productId(1))
                .name("Product-1")
                .price(101)
                .build())
                .ignoreElement()
                .blockingAwait();

        products.update(Product
                .builder()
                .key(UniqueId.productId(2))
                .name("Product-2")
                .price(99)
                .build())
                .ignoreElement()
                .blockingAwait();


        TestObserver<Notification<Product>> productObserver = products
                .query()
                .where(Product.$.price.greaterThan(100))
                .queryAndObserve()
                .doOnNext(System.out::println)
                .test();

        productObserver
                .assertOf(countAtLeast(1))
                .assertValueAt(0, Notification::isCreate)
                .assertValueAt(0, n -> requireNonNull(n.newValue()).price() == 101);

        productObserver
                .assertOf(countLessThan(2))
                .assertTimeout();

        products.update(Product
                .builder()
                .key(UniqueId.productId(2))
                .name("Product-2")
                .price(102)
                .build())
                .ignoreElement().blockingAwait();

        productObserver
                .assertOf(countExactly(2))
                .assertValueAt(1, Notification::isCreate)
                .assertValueAt(1, n -> requireNonNull(n.newValue()).price() == 102);

        products.update(Product
                .builder()
                .key(UniqueId.productId(1))
                .name("Product-1")
                .price(95)
                .build())
                .ignoreElement().blockingAwait();

        productObserver
                .assertOf(countExactly(3))
                .assertValueAt(2, NotificationPrototype::isDelete)
                .assertValueAt(2, n -> requireNonNull(n.oldValue()).price() == 101);

        products.update(Product
                .builder()
                .key(UniqueId.productId(3))
                .name("Product-3")
                .price(92)
                .build())
                .ignoreElement().blockingAwait();

        productObserver.assertOf(countLessThan(4));
    }

    @Test
    public void testDistinctSelect() {
        EntitySet<UniqueId, Product> products = repository.entities(Product.metaClass);
        products.update(Products.createMany(20)).blockingAwait();
        products.query()
                .selectDistinct(Product.$.inventory.name)
                .retrieve()
                .doOnNext(System.out::println)
                .toList()
                .test()
                .awaitCount(1)
                .assertValue(l -> l.size() == 2)
                .assertValue(l -> l.contains("Inventory-0"));

        products.query()
                .selectDistinct(Product.$.inventory)
                .retrieve(Inventory.$.name)
                .toList()
                .test()
                .awaitCount(1)
                .assertValue(l -> l.size() == 2);
    }

    @Test
    public void testAggregateMinDate() {
        List<Product> productList = ImmutableList.copyOf(Products.createMany(10));
        EntitySet<UniqueId, Product> products = repository.entities(Product.metaClass);
        products.update(productList).blockingAwait();
        Date maxDate = products.query()
                .select(Product.$.productionDate)
                .aggregate(Aggregator.max())
                .blockingGet();

        Date expectedMinDate = productList.get(0).productionDate();
        Date expectedMaxDate = productList.get(productList.size() - 1).productionDate();

        Date minDate = products.query()
                .select(Product.$.productionDate)
                .aggregate(Aggregator.min())
                .blockingGet();

        Assert.assertEquals(expectedMaxDate, maxDate);
        Assert.assertEquals(expectedMinDate, minDate);
    }

    @Test
    public void testAggregateAveragePrice() {
        EntitySet<UniqueId, Product> products = repository.entities(Product.metaClass);
        products.update(Products.createMany(10)).blockingAwait();
        double averagePrice = products.query()
                .select(Product.$.price)
                .aggregate(Aggregator.average())
                .blockingGet();

        Assert.assertEquals(116.2, averagePrice, 0.0001);
    }

    @Test
    public void testFilterByDate() throws InterruptedException {
        List<Product> productList = ImmutableList.copyOf(Products.createMany(10));
        EntitySet<UniqueId, Product> products = repository.entities(Product.metaClass);
        products.update(productList).blockingAwait();
        products.query()
                .where(Product.$.productionDate.lessOrEqual(productList.get(4).productionDate()))
                .retrieve()
                .test()
                .await()
                .assertValueCount(5);
    }

    @Test
    public void testQueryByNestedEmbeddedObject() throws InterruptedException {
        EntitySet<UniqueId, Product> products = repository.entities(Product.metaClass);
        products.update(Products.createMany(20)).blockingAwait();
        UniqueId vendorId = UniqueId.vendorId(2);
        products.query()
                .where(Product.$.vendor.id.eq(vendorId))
                .retrieve(Product.$.vendor)
                .test()
                .await()
                .assertValueCount(5)
                .assertValueAt(0, p -> requireNonNull(p.vendor()).id().equals(vendorId));

        products.query()
                .where(Product.$.vendor.id.in(vendorId))
                .retrieve(Product.$.vendor)
                .test()
                .await()
                .assertValueCount(5)
                .assertValueAt(0, p -> requireNonNull(p.vendor()).id().equals(vendorId));
    }

    @Test @Ignore
    public void testObserveAsListEmptyCollection() {
        products.query().observeAsList().test().awaitCount(1)
                .assertValueCount(1)
                .assertValue(List::isEmpty);

        products.update(Products.createOne(1)).ignoreElement().blockingAwait();

        TestObserver<List<Product>> productObserver = products.query().where(Product.$.key.id.greaterThan(1))
                .observeAsList()
                .doOnNext(l -> System.out.println("List received: " + l.size()))
                .test()
                .assertSubscribed();

        productObserver.awaitCount(1)
                .assertNoErrors()
                .assertValueCount(1)
                .assertValue(List::isEmpty);

        products.update(Products.createOne(2)).ignoreElement().blockingAwait();
        productObserver.awaitCount(2)
                .assertValueCount(2)
                .assertValueAt(1, l -> l.size() == 1);

        products.update(Products.createOne(3)).ignoreElement().blockingAwait();
        products.query()
                .where(Product.$.key.id.greaterThan(1))
                .skip(1)
                .limit(2)
                .observeAsList()
                .test()
                .awaitCount(1)
                .assertValueCount(1)
                .assertValue(l -> l.size() == 1)
                .assertValue(l -> l.get(0).key().id() == 3);
    }

    @Test
    @UseLogLevel(LogLevel.TRACE)
    public void testObserveAsList() {
        products.update(Products.createMany(10)).blockingAwait();
        TestObserver<List<Product>> productTestObserver = products.query()
                .orderBy(Product.$.name)
                .orderByDescending(Product.$.price)
                .limit(3)
                .skip(2)
                .liveSelect()
                .properties(Product.$.name, Product.$.price)
                .observeAs(Notifications.toSlidingList())
                .test()
                .assertOf(countAtLeast(1))
                .assertValueAt(0, l -> l.size() == 3)
                .assertValueAt(0, l -> Objects.equals(l.get(0).name(), "Product-2"))
                .assertValueAt(0, l -> Objects.equals(l.get(2).name(), "Product-4"));

        products.update(Arrays.asList(
                Product.builder()
                        .name("Product-3-1")
                        .key(UniqueId.productId(11))
                        .price(100)
                        .type(ProductEntity.Type.ComputeHardware)
                        .build(),
                Product.builder()
                        .name("Product-1-1")
                        .key(UniqueId.productId(13))
                        .price(100)
                        .type(ProductEntity.Type.ComputeHardware)
                        .build(),
                Product.builder()
                        .name("Product-5-1")
                        .key(UniqueId.productId(12))
                        .price(100)
                        .type(ProductEntity.Type.ComputeHardware)
                        .build()))
                .blockingAwait();

        productTestObserver
                .assertOf(countAtLeast(2))
                .assertValueAt(1, l -> l.size() == 3)
                .assertValueAt(1, l -> Objects.equals(l.get(0).name(), "Product-2"))
                .assertValueAt(1, l -> Objects.equals(l.get(1).name(), "Product-3"))
                .assertValueAt(1, l -> Objects.equals(l.get(2).name(), "Product-3-1"));
    }

    @Test
    public void testObserveAsSlidingListCorrectCount() {
        products.update(Products.createMany(100)).blockingAwait();
        AtomicLong lastCount = new AtomicLong();
        QueryTransformer<Product, List<Product>> toListTransformer = Notifications.toList();
        List<Product> productList = products.query()
                .where(Product.$.key.id.greaterThan(10))
                .skip(10)
                .limit(20)
                .observeAs(new QueryTransformer<Product, List<Product>>() {
                    @Override
                    public <K, S> ObservableTransformer<List<Notification<S>>, List<Product>> transformer(QueryInfo<K, S, Product> query, AtomicLong count) {
                        return src -> src.compose(toListTransformer.transformer(query, count))
                                .doOnNext(n -> lastCount.set(count.get()));
                    }
                })
                .take(1)
                .blockingFirst();
        Assert.assertEquals(20, productList.size());
        Assert.assertEquals(89, lastCount.get());
    }

    @Test
    public void testObserveAsListWithProperties() {
        EntitySet<UniqueId, Product> products = repository.entities(Product.metaClass);
        products.update(Products.createMany(10)).blockingAwait();
        products.query()
                .orderBy(Product.$.name)
                .limit(3)
                .skip(2)
                .observeAsList(Product.$.key, Product.$.name)
                .doOnNext(l -> {
                    System.out.println("List received: ");
                    l.forEach(System.out::println);
                })
                .test()
                .assertOf(countAtLeast(1))
                .assertValue(l -> l.size() == 3)
                .assertValue(l -> Objects.isNull(l.get(0).type()))
                .assertValue(l -> Objects.equals(l.get(0).name(), "Product-2"))
                .assertValue(l -> Objects.equals(l.get(2).name(), "Product-4"));
    }

    @Test
    public void testObserveAsListWithPredicate() {
        EntitySet<UniqueId, Product> products = repository.entities(Product.metaClass);
        Product product2 = Product.builder()
                .name("Product-2")
                .inventory(Inventory.builder()
                        .id(UniqueId.inventoryId(1))
                        .name("Inventory-1")
                        .build())
                .key(UniqueId.productId(2))
                .price(100)
                .type(ProductEntity.Type.ComputeHardware)
                .build();

        products.update(product2).ignoreElement().blockingAwait();

        TestObserver<List<Product>> productTestObserver = products.query()
                .where(Product.$.price.eq(100))

                .observeAsList(Product.$.name, Product.$.inventory.name)
                .doOnNext(l -> {
                    System.out.println("List received: ");
                    l.forEach(System.out::println);
                })
                .test()
                .assertOf(countAtLeast(1))
                .assertValueAt(0, l -> l.size() == 1);


        Product product1 = Product.builder()
                .name("Product-1")
                .inventory(Inventory.builder()
                        .name("Inventory-1")
                        .id(UniqueId.inventoryId(1))
                        .build())
                .key(UniqueId.productId(1))
                .price(100)
                .type(ProductEntity.Type.ComputeHardware)
                .build();

        products.update(product1).ignoreElement().blockingAwait();

        productTestObserver
                .assertOf(countAtLeast(2))
                .assertValueAt(1, l -> l.size() == 2)
                .assertValueAt(1, l -> Objects.equals(l.get(0).name(), "Product-2"))
                .assertValueAt(1, l -> Objects.equals(l.get(1).name(), "Product-1"))
                .assertValueAt(1, l -> Optional.ofNullable(l.get(0).inventory()).map(Inventory::name).map("Inventory-1"::equals).orElse(false));

        products.update(product1.toBuilder().name("Product-1-1").build()).ignoreElement().blockingAwait();

        productTestObserver
                .assertOf(countAtLeast(3))
                .assertValueAt(2, l -> l.size() == 2);
    }

    @Test @UseLogLevel(LogLevel.TRACE)
    public void testRetrieveAsListWithProperties() throws InterruptedException {
        EntitySet<UniqueId, Product> products = repository.entities(Product.metaClass);
        products.update(Products.createMany(10)).blockingAwait();
        products.query()
                .orderBy(Product.$.name)
                .limit(3)
                .skip(2)
                .retrieveAsList(Product.$.key, Product.$.name)
                .test()
                .await()
                .assertNoErrors()
                .assertComplete()
                .assertValue(l -> l.size() == 3)
                .assertValue(l -> Objects.isNull(l.get(0).type()))
                .assertValue(l -> Objects.equals(l.get(0).name(), "Product-2"))
                .assertValue(l -> Objects.equals(l.get(2).name(), "Product-4"));
    }

    @Test
    public void testLiveQueryWithProjection() throws InterruptedException {
        repository.entities(Product.metaClass)
                .update(Products.createMany(10))
                .test()
                .awaitCount(10)
                .assertNoErrors();

        TestObserver<Notification<Product>> productNameChanges = repository
                .entities(Product.metaClass)
                .query()
                .liveSelect()
                .queryAndObserve(Product.$.name)
                .doOnNext(System.out::println)
                .test();

        TestObserver<Notification<Product>> productTypeChanges = repository
                .entities(Product.metaClass)
                .query()
                .liveSelect()
                .queryAndObserve(Product.$.type)
                .doOnNext(System.out::println)
                .test();

        TestObserver<Notification<Product>> productNameAndTypeChanges = repository
                .entities(Product.metaClass)
                .query()
                .liveSelect()
                .queryAndObserve(Product.$.name, Product.$.type)
                .doOnNext(System.out::println)
                .test();

        productNameChanges.assertOf(countExactly(10));
        productNameAndTypeChanges.assertOf(countExactly(10));
        productTypeChanges.assertOf(countExactly(10));

        repository.entities(Product.metaClass)
                .update(UniqueId.productId(1), productMaybe -> productMaybe
                        .map(p -> p.toBuilder().name(p.name() + " - new name").build()))
                .test()
                .await()
                .assertValue(p -> "Product-1 - new name".equals(p.get().name()))
                .assertNoErrors();

        productNameChanges.assertOf(countExactly(11));
        productNameAndTypeChanges.assertOf(countExactly(11));
        productTypeChanges.assertOf(countExactly(10));

        productNameChanges
                .assertValueAt(10, NotificationPrototype::isModify)
                .assertValueAt(10, n -> "Product-1".equals(requireNonNull(n.oldValue()).name()))
                .assertValueAt(10, n -> "Product-1 - new name".equals(requireNonNull(n.newValue()).name()));

        repository.entities(Product.metaClass)
                .update(UniqueId.productId(1), productMaybe -> productMaybe
                        .map(p -> p.toBuilder().type(ProductEntity.Type.ComputerSoftware).build()))
                .test()
                .await()
                .assertValue(p -> ProductEntity.Type.ComputerSoftware.equals(p.get().type()))
                .assertNoErrors();

        productTypeChanges.assertOf(countExactly(11));
        productNameAndTypeChanges.assertOf(countExactly(12));
        productNameChanges.assertValueCount(11);

        productTypeChanges
                .assertValueAt(10, NotificationPrototype::isModify)
                .assertValueAt(10, n -> ProductEntity.Type.ComputeHardware.equals(requireNonNull(n.oldValue()).type()))
                .assertValueAt(10, n -> ProductEntity.Type.ComputerSoftware.equals(requireNonNull(n.newValue()).type()));
    }

    @Test
    @UseLogLevel(LogLevel.TRACE)
    public void testLiveSelectWithMapping() throws InterruptedException {
        repository.entities(Product.metaClass)
                .update(Products.createMany(10))
                .test()
                .await()
                .assertNoErrors();

        TestObserver<List<Inventory>> inventoriesObserver = repository.entities(Product.metaClass)
                .query()
                .liveSelect(Product.$.inventory)
                .observeAs(Notifications.toList())
                .doOnNext(n -> n.forEach(System.out::println))
                .debounce(500, TimeUnit.MILLISECONDS)
                .test()
                .assertSubscribed();

        inventoriesObserver
                .assertOf(countAtLeast(1))
                .assertNoTimeout()
                .assertNoErrors()
                .assertValueAt(0, l -> l.size() == 10)
                .assertValueAt(0, l -> l.get(0) != null);

        repository.entities(Product.metaClass)
                .update(Products.createMany(11))
                .blockingAwait();

        inventoriesObserver
                .assertOf(countAtLeast(2))
                .assertNoTimeout()
                .assertNoErrors()
                .assertValueAt(1, l -> l.size() == 11)
                .assertValueAt(1, l -> l.get(10) != null);
    }

    @Test
    public void testLiveAggregateWithMapping() {
        TestObserver<Long> inventoriesObserver = repository.entities(Product.metaClass)
                .query()
                .map(Product.$.inventory)
                .observeCount()
                .filter(c -> c > 0)
                .test()
                .assertSubscribed();

        repository.entities(Product.metaClass)
                .update(Products.createMany(10))
                .test()
                .awaitCount(10)
                .assertNoErrors();

        inventoriesObserver
                .assertOf(countAtLeast(1))
                .assertNoTimeout()
                .assertNoErrors();
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testLiveQueryThenUpdate() {
        repository.entities(Product.metaClass)
                .update(Products.createMany(10))
                .blockingAwait();

        TestObserver<Notification<Product>> productObserver = repository.entities(Product.metaClass)
                .query()
                .where(Product.$.key.id.lessThan(5))
                .liveSelect()
                .observe(Product.$.name, Product.$.price, Product.$.productionDate)
                .doOnNext(System.out::println)
                .test()
                .assertSubscribed();

        Product product1 = repository.entities(Product.metaClass)
                .find(UniqueId.productId(1), Product.$.name, Product.$.productionDate)
                .blockingGet();

        Product product8 = repository.entities(Product.metaClass)
                .find(UniqueId.productId(8), Product.$.name, Product.$.productionDate)
                .blockingGet();

        repository.entities(Product.metaClass)
                .update(product8.toBuilder().name(product8.name() + " - Updated").build())
                .ignoreElement()
                .blockingAwait();

        repository.entities(Product.metaClass)
                .update(product1.toBuilder().name(product1.name() + " - Updated").build())
                .ignoreElement()
                .blockingAwait();

        productObserver.awaitCount(1)
                .assertValueAt(0, NotificationPrototype::isModify)
                .assertValueAt(0, p -> p.oldValue().name().equals("Product-1"))
                .assertValueAt(0, p -> p.newValue().name().equals("Product-1 - Updated"));

        repository.entities(Product.metaClass)
                .update(product1.toBuilder().productionDate(new Date(product1.productionDate().getTime() + 1)).build())
                .ignoreElement()
                .blockingAwait();

        repository.entities(Product.metaClass)
                .update(product1.toBuilder().price(product1.price() + 1).build())
                .ignoreElement()
                .blockingAwait();

        productObserver.awaitCount(2)
                .assertValueAt(1, NotificationPrototype::isModify)
                .assertValueAt(1, p -> p.newValue().productionDate().getTime() - p.oldValue().productionDate().getTime() == 1);
    }

    @SuppressWarnings("ReactiveStreamsNullableInLambdaInTransform")
    @Test
    @UseLogLevel(LogLevel.TRACE)
    public void testRetrieveWithReferenceProperty() {
        TestObserver<Product> productTestObserver = repository.entities(Product.metaClass)
                .query()
                .limit(1)
                .liveSelect()
                .properties(Product.$.inventory)
                .observe()
                .filter(Notification::isCreate)
                .map(Notification::newValue)
                .take(1)
                .test();

        productTestObserver.assertValueCount(0);

        repository.entities(Product.metaClass)
                .update(Products.createMany(10))
                .blockingAwait();

        productTestObserver
                .assertOf(countExactly(1))
                .assertValue(p -> p.inventory() != null);

        repository.entities(Product.metaClass)
                .query()
                .limit(1)
                .select()
                .properties(Product.$.inventory)
                .retrieve()
                .take(1)
                .test()
                .assertOf(countExactly(1))
                .assertValue(p -> p.inventory() != null);
    }

    @Test
    @UseLogLevel(LogLevel.INFO)
    public void testLargeUpdate() throws InterruptedException {

        int count = Optional.ofNullable(System.getProperty("test.testLargeUpdate.count"))
                .map(Integer::valueOf)
                .orElse(20000);
        repository.entities(Product.metaClass).update(Products.createMany(count))
                .test()
                .await()
                .assertNoErrors()
                .assertComplete();

        Assert.assertEquals(Long.valueOf(count), repository.entities(Product.metaClass).query().count().blockingGet());
    }

    @Test
    @UseLogLevel(LogLevel.DEBUG)
    public void testLargeUpdateNoReferences() throws InterruptedException {
        repository.entities(Inventory.metaClass).update(Streams
                .fromIterable(Products.createMany(1, 100000, 1))
                .map(Product::inventory)
                .collect(Collectors.toList()))
                .test()
                .await()
                .assertNoErrors()
                .assertComplete();
    }

    @Test
    public void testSearchWithSpecialSymbols() throws InterruptedException {
        repository.entities(Product.metaClass).update(Products
                .createOne()
                .toBuilder()
                .name("Product-1 with special symbols, for example: colon, coma & ampersand")
                .build())
                .ignoreElement()
                .blockingAwait();

        repository.entities(Product.metaClass)
                .query()
                .where(Product.$.searchText("example: colon"))
                .retrieve()
                .test()
                .await()
                .assertNoErrors()
                .assertValueCount(1);
    }

    @Test
    public void testAggregateOnEmptySet() throws InterruptedException {
        Assert.assertEquals(Long.valueOf(0), repository.entities(Product.metaClass)
                .query()
                .select()
                .aggregate(Aggregator.count())
                .blockingGet());

        repository.entities(Product.metaClass)
                .query()
                .select(Product.$.name)
                .aggregate(Aggregator.max())
                .test()
                .await()
                .assertValueCount(0)
                .assertComplete();
    }

    @Test @Ignore
    public void testObserveCount() {
        TestObserver<Long> testObserver = repository.entities(Product.metaClass)
                .query()
                .where(Product.$.key.id.eq(2))
                .observeCount()
                .test()
                .assertSubscribed();

        testObserver.awaitCount(1)
                .assertValueCount(1)
                .assertValueAt(0, 0L);

        repository.entities(Product.metaClass).update(Products.createOne(2)).ignoreElement().blockingAwait();

        testObserver
                .assertOf(TestUtils.countAtLeast(2, Duration.ofSeconds(10)))
                .assertValueAt(1, 1L);
    }

    @Test
    @UseLogLevel(LogLevel.DEBUG)
    @Ignore
    public void testUpdatingDeletedObjectShouldNotAddObjectIntoRepo() throws InterruptedException {

        EntitySet<UniqueId, Product> entities = repository.entities(Product.metaClass);
        Product product = Products
                .createOne()
                .toBuilder()
                .name("TestProduct")
                .build();

        // add stub product to the repo
        entities.update(product)
                .ignoreElement()
                .blockingAwait();

        AtomicBoolean deleted = new AtomicBoolean(false);
        entities.update(product.key(), productMaybe -> productMaybe.map(p -> {
            // deleting the object from the repo just before update
            if (deleted.compareAndSet(false, true))
                entities.delete(p.key()).blockingAwait();

            return p.toBuilder().name("updatedName").build();
        }))
                .test()
                .await()
                .assertValueCount(0)
                .assertComplete();

        //verify that there is nothing in the collection
        entities.query()
                .count()
                .test()
                .await()
                .assertValue(0L);
    }

    @Test
    @UseLogLevel(LogLevel.TRACE)
    public void testMassiveInsertBatch() {
        int count = 1;

        Stopwatch stopwatch = Stopwatch.createStarted();

        EntitySet<UniqueId, Product> productEntitySets = repository.entities(Product.metaClass);
        Iterable<Product> products = Products.createMany(count);
        productEntitySets.update(products).blockingAwait();

        System.out.println("Elapsed time for 1st insert: " + stopwatch.elapsed().toMillis() / 1000 + "s");

        Assert.assertEquals(Long.valueOf(count), repository.entities(Product.metaClass).query().count().blockingGet());

        products = Products.createMany(count, count, 10);
        stopwatch.reset().start();
        productEntitySets.update(products).blockingAwait();

        System.out.println("Elapsed time for 2nd insert: " + stopwatch.elapsed().toMillis() / 1000 + "s");

        Assert.assertEquals(Long.valueOf(count * 2), repository.entities(Product.metaClass).query().count().blockingGet());
    }

    @Test @Ignore
    public void testMassiveUpdateOneByOne() {
        long count = 10000;

        Stopwatch stopwatch = Stopwatch.createStarted();

        EntitySet<UniqueId, Product> products = repository.entities(Product.metaClass);
        Observable.fromIterable(Products.createMany((int)count))
                .flatMapSingle(products::update)
                .ignoreElements()
                .blockingAwait();

        stopwatch.stop();
        System.out.println("Elapsed time: " + stopwatch.elapsed().toMillis() / 1000 + "s");

        Assert.assertEquals(Long.valueOf(count), repository.entities(Product.metaClass).query().count().blockingGet());
    }

    @Test
    public void testUnsubscribeOnClose() {
        TestObserver<Notification<Product>> productTestObserver1 = repository.entities(Product.metaClass)
                .observe()
                .test();

        TestObserver<Notification<Product>> productTestObserver2 = repository.entities(Product.metaClass)
                .queryAndObserve()
                .test();

        TestObserver<Long> productTestObserver3 = repository.entities(Product.metaClass)
                .query().observeCount()
                .test();

        log.info("Adding entry");
        repository.entities(Product.metaClass).update(Products.createOne()).ignoreElement().blockingAwait();

        log.info("Waiting for observables update");

        log.info("Waiting for observer1 update...");
        productTestObserver1.awaitCount(1).assertNotComplete();

        log.info("Waiting for observer2 update...");
        productTestObserver2.awaitCount(1).assertNotComplete();

        log.info("Waiting for observer3 update...");
        productTestObserver3.awaitCount(1).assertNotComplete();

        log.info("Closing repository");
        repository.close();

        log.info("Waiting for observer1 complete");
        productTestObserver1.awaitDone(10000, TimeUnit.MILLISECONDS).assertComplete().assertNoErrors();

        log.info("Waiting for observer2 complete");
        productTestObserver2.awaitDone(10000, TimeUnit.MILLISECONDS).assertComplete().assertNoErrors();

        log.info("Waiting for observer3 complete");
        productTestObserver3.awaitDone(10000, TimeUnit.MILLISECONDS).assertComplete().assertNoErrors();
    }

    @Test
    @UseLogLevel(LogLevel.TRACE)
    public void testSearchTextLiveQuery() {
        TestObserver<Notification<Product>> testObserver = products.query()
                .where(Product.$.searchText("Product-0"))
                .liveSelect()
                .observe()
                .test();

        products.update(Products.createOne(0)).ignoreElement().blockingAwait();
        testObserver.awaitCount(1)
                .assertValueCount(1)
                .assertValueAt(0, NotificationPrototype::isCreate);

        products.update(Products.createOne(0).toBuilder().name("Product Zero").build()).ignoreElement().blockingAwait();
        testObserver
                .awaitCount(2)
                .assertValueCount(2)
                .assertValueAt(1, NotificationPrototype::isDelete);
    }

    @Test @Ignore
    public void testSearchTextPerformance() {
        products.update(Products.createMany(100000)).blockingAwait();
        Stopwatch stopwatch = Stopwatch.createStarted();
        products.query()
                .where(Product.$.searchText("ory-1"))
                .retrieve()
                .ignoreElements()
                .blockingAwait();
        System.out.println(MoreStrings.format("Query took {}ms", stopwatch.elapsed().toMillis()));
    }

    @SuppressWarnings({"unchecked", "ConstantConditions"})
    @Test
    @UseLogLevel(LogLevel.TRACE)
    public void testReferencePropertiesLiveQuery() {
        products.update(Products.createMany(100)).blockingAwait();
        TestObserver<Notification<Product>> testObserver = products.queryAndObserve(Product.$.name, Product.$.inventory.name)
                .doOnNext(System.out::println)
                .test();

        testObserver.awaitCount(100)
                .assertValueCount(100);

        repository.entities(Inventory.metaClass)
                .update(Inventory.builder()
                        .id(UniqueId.inventoryId(1))
                        .name("Inventory-1 - updated")
                        .build())
                .ignoreElement()
                .blockingAwait();

        testObserver.awaitCount(110)
                .assertValueCount(110)
                .assertValueAt(100, NotificationPrototype::isModify)
                .assertValueAt(100, n -> n.oldValue().inventory() != null && n.newValue().inventory() != null)
                .assertValueAt(100, n -> !Objects.equals(n.oldValue().inventory(), n.newValue().inventory()))
                .assertValueAt(100, n -> !Objects.equals(n.oldValue().inventory().name(), n.newValue().inventory().name()))
                .assertValueAt(100, n -> Objects.equals("Inventory-1", n.oldValue().inventory().name()))
                .assertValueAt(100, n -> Objects.equals("Inventory-1 - updated", n.newValue().inventory().name()));
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    @UseLogLevel(LogLevel.TRACE)
    public void testReferencePropertiesLiveQuerySingleProduct() {
        final int count = 3;
        List<Product> productList = ImmutableList.copyOf(Products.createMany(count));
        products.update(productList).blockingAwait();
        TestObserver<Notification<Product>> testObserver = products.queryAndObserve(Product.$.name, Product.$.inventory.name)
                .doOnNext(System.out::println)
                .test();

        testObserver.awaitCount(count)
                .assertValueCount(count);

        repository.entities(Inventory.metaClass)
                .update(Inventory.builder()
                        .id(UniqueId.inventoryId(0))
                        .name("Inventory-0 - updated")
                        .build())
                .ignoreElement()
                .blockingAwait();

        testObserver.awaitCount(count * 2)
                .assertValueCount(count * 2)
                .assertValueAt(count, NotificationPrototype::isModify)
                .assertValueAt(count, n -> n.oldValue().inventory() != null && n.newValue().inventory() != null)
                .assertValueAt(count, n -> !Objects.equals(n.oldValue().inventory(), n.newValue().inventory()))
                .assertValueAt(count, n -> !Objects.equals(n.oldValue().inventory().name(), n.newValue().inventory().name()))
                .assertValueAt(count, n -> Objects.equals("Inventory-0", n.oldValue().inventory().name()))
                .assertValueAt(count, n -> Objects.equals("Inventory-0 - updated", n.newValue().inventory().name()));
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    @Ignore
    @UseLogLevel(logger = "com.slimgears.rxrepo ", value = LogLevel.TRACE)
    public void testAddProductThenUpdateInventoryInOrder() throws InterruptedException {
        Map<UniqueId, Product> knownProducts = new ConcurrentHashMap<>();
        final int productCount = 2000;
        final int inventoryCount = productCount / 10;

        AtomicLong lastSeqNum = new AtomicLong();

        TestObserver<Notification<Product>> productTestObserver = products.observe(Product.$.name, Product.$.inventory.name)
                .doOnNext(n -> {
                    System.out.println(n);
                    if (n.isCreate()) {
                        Assert.assertTrue(n.sequenceNumber() > lastSeqNum.get());
                        lastSeqNum.set(n.sequenceNumber());
                        knownProducts.put(n.newValue().key(), n.newValue());
                    } else if (n.isModify() || n.isDelete()) {
                        Assert.assertTrue(knownProducts.containsKey(n.oldValue().key()));
                    }
                })
                .filter(n -> n.isCreate() || n.isModify())
                .take((int)(productCount * 1.1))
                .test();

        Observable.fromIterable(Products.createMany(productCount))
                .flatMapSingle(products::update)
                .ignoreElements()
                .mergeWith(Observable.range(0, inventoryCount)
                        .flatMapMaybe(i -> inventories.update(UniqueId.inventoryId(i), maybeInv -> maybeInv
                                .map(inv -> inv.toBuilder()
                                        .name(inv.name() + " - updated")
                                        .build())))
                        .ignoreElements())
                .blockingAwait();

        productTestObserver
                .await()
                .assertNoErrors();
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    @UseLogLevel(logger = "com.slimgears.rxrepo.orientdb.OrientDbLiveQueryListener", value = LogLevel.TRACE)
    public void testAddProductThenUpdateInventoryInOrderQueryAndObserve() throws InterruptedException {
        Map<UniqueId, Product> knownProducts = new ConcurrentHashMap<>();
        final int productCount = 2000;
        final int inventoryCount = productCount / 10;

        AtomicLong lastSeqNum = new AtomicLong();

        products.update(Products.createMany(productCount))
                .blockingAwait();

        TestObserver<Notification<Product>> productTestObserver = products
                .queryAndObserve(Product.$.name, Product.$.inventory.name)
                .doOnNext(System.out::println)
                .doOnNext(n -> {
                    if (n.isCreate()) {
                        knownProducts.put(n.newValue().key(), n.newValue());
                    } else if (n.isModify() || n.isDelete()) {
                        Assert.assertTrue(knownProducts.containsKey(n.oldValue().key()));
                    }
                })
                .filter(n -> n.isCreate() || n.isModify())
                .take(productCount + productCount / 10)
                .test();

        Observable.range(0, inventoryCount)
                .flatMapMaybe(i -> inventories.update(UniqueId.inventoryId(i), maybeInv -> maybeInv
                        .map(inv -> inv.toBuilder()
                                .name(inv.name() + " - updated")
                                .build())))
                .ignoreElements()
                .blockingAwait();

        productTestObserver
                .await()
                .assertNoErrors();
    }

    @Test
    @UseLogLevel(LogLevel.TRACE)
    public void testReferencePropertiesLiveQueryWithPredicate() {
        Product product = Products.createOne();
        products.update(product).ignoreElement().blockingAwait();
        TestObserver<Notification<Product>> testObserver = products
                .query()
                .where(Product.$.inventory.name.startsWith("Inventory")
                        .and(Product.$.inventory.manufacturer.name.eq("Manufacturer-0")))
                .queryAndObserve(Product.$.name)
                .doOnNext(System.out::println)
                .test();

        testObserver.assertOf(countExactly(1))
                .assertValueAt(0, NotificationPrototype::isCreate);

        repository.entities(Manufacturer.metaClass)
                .update(requireNonNull(requireNonNull(product.inventory()).manufacturer())
                        .toBuilder()
                        .name("Updated Manufacturer 0").build())
                .ignoreElement()
                .blockingAwait();

        testObserver.assertOf(countExactly(2))
                .assertValueAt(1, NotificationPrototype::isDelete);

    }
}
