package com.slimgears.rxrepo.mongodb;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;
import com.mongodb.reactivestreams.client.*;
import com.slimgears.rxrepo.mongodb.adapter.StandardCodecs;
import com.slimgears.rxrepo.test.*;
import io.reactivex.Completable;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.observers.TestObserver;
import io.reactivex.subjects.CompletableSubject;
import org.bson.Document;
import org.junit.*;
import org.reactivestreams.Publisher;

import java.util.Arrays;
import java.util.List;

import static com.slimgears.rxrepo.test.Products.createMany;
import static com.slimgears.rxrepo.test.Products.createOne;
import static com.slimgears.rxrepo.test.TestUtils.countExactly;

@Ignore
public class MongoDbClientTest {
    private static AutoCloseable mongoProcess;
    private MongoClient mongoClient;
    private MongoDatabase mongoDatabase;
    private MongoCollection<Product> collection;

    @BeforeClass
    public static void setUpClass() {
        mongoProcess = MongoTestUtils.startMongo();
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        if (mongoProcess != null) {
            mongoProcess.close();
        }
    }

    @Before
    public void setUp() {
        mongoClient = MongoClients
                .create(MongoClientSettings
                        .builder()
                        .applyToConnectionPoolSettings(b -> b.maxSize(200))
                        .codecRegistry(StandardCodecs.registry())
                        .applyConnectionString(MongoTestUtils.connectionString)
                        .build());

        mongoDatabase = mongoClient.getDatabase("test-repository");
        collection = mongoDatabase.getCollection(Product.metaClass.simpleName(), Product.class);
    }

    @After
    public void tearDown() {
        System.out.println("Closing mongo client...");
        await(mongoDatabase.drop());
        mongoClient.close();
        System.out.println("Done");
    }

    @Test
    public void testBasicFunctionality() {
        Observable.fromIterable(createMany(10))
                .flatMapCompletable(p -> Completable.fromPublisher(collection
                        .replaceOne(new Document().append("_id", p.key()), p, new ReplaceOptions().upsert(true))))
                .blockingAwait();

        long count = Single
                .fromPublisher(collection.countDocuments())
                .blockingGet();

        Assert.assertEquals(10, count);

        List<Product> documents = Observable.fromPublisher(mongoDatabase.getCollection("Product", Product.class)
                .find())
                .toList()
                .blockingGet();

        Assert.assertEquals(10, documents.size());

        List<Product> filteredProducts = Observable.fromPublisher(collection.find(MongoPipeline.expr(Product.$.price.greaterOrEqual(106))))
                .toList()
                .blockingGet();

        Assert.assertEquals(8, filteredProducts.size());
    }

    @Test @Ignore
    public void testWatchCollection() {
        CompletableSubject subscribed = CompletableSubject.create();

        TestObserver<ChangeStreamDocument<Document>> testObserver = Observable
                .fromPublisher(collection.watch().fullDocument(FullDocument.UPDATE_LOOKUP))
                .doOnNext(System.out::println)
                .doOnSubscribe(d -> subscribed.onComplete())
                .test();

        Product product = createOne();

        subscribed.blockingAwait();

        Completable
                .fromPublisher(collection.insertOne(product))
                .blockingAwait();

        testObserver.assertOf(countExactly(1));

        Completable
                .fromPublisher(collection.updateOne(MongoPipeline.filterFor(Product.metaClass, product), new Document("$set", product.toBuilder().price(product.price() + 1).build())))
                .blockingAwait();

        testObserver.assertOf(countExactly(2));
    }

    @Test @Ignore
    public void testObjectWithReferencesFromEntity() {
        ProductDescription productDescription = ProductDescription
                .builder()
                .key(UniqueId.productDescriptionId(1))
                .product(Product
                        .builder()
                        .key(UniqueId.productId(1))
                        .name("Product 1")
                        .type(ProductEntity.Type.ComputeHardware)
                        .price(100)
                        .inventory(Inventory
                                .builder()
                                .name("Inventory 1")
                                .id(UniqueId.inventoryId(1))
                                .build())
                        .build())
                .build();

        MongoCollection<ProductDescription> productDescriptions = mongoDatabase.getCollection(ProductDescription.metaClass.simpleName(), ProductDescription.class);
        MongoCollection<Product> products = mongoDatabase.getCollection(Product.metaClass.simpleName(), Product.class);
        MongoCollection<Inventory> inventories = mongoDatabase.getCollection(Inventory.metaClass.simpleName(), Inventory.class);
        Completable.fromPublisher(inventories.insertOne(productDescription.product().inventory())).blockingAwait();
        Completable.fromPublisher(products.insertOne(productDescription.product())).blockingAwait();
        Completable.fromPublisher(productDescriptions.insertOne(productDescription)).blockingAwait();

        AggregatePublisher<ProductDescription> foundProducts = productDescriptions.aggregate(MongoPipeline
                .builder()
                .lookupAndUnwindReferences(ProductDescription.metaClass)
                .build(), ProductDescription.class);
        ProductDescription foundProductDescription = Observable
                .fromPublisher(foundProducts)
                .firstElement()
                .blockingGet();

        Assert.assertEquals(productDescription, foundProductDescription);
    }

    @Test
    public void testCountAggregation() {
        MongoCollection<Document> storages = mongoDatabase.getCollection(Storage.metaClass.simpleName());
        MongoCollection<Document> products = mongoDatabase.getCollection(Product.metaClass.simpleName());
        MongoCollection<Document> inventories = mongoDatabase.getCollection(Inventory.metaClass.simpleName());

        await(inventories.insertOne(
                new Document("_id", 1)
                        .append("name", "Inventory 1")));

        await(products.insertOne(
                new Document("_id", 1)
                        .append("name", "Product 2")
                        .append("inventory", 1)));

        await(storages.insertOne(
                new Document("_id", 1)
                        .append("product", 1)));

        Observable.fromPublisher(storages
                .aggregate(Arrays.asList(
                        new Document("$lookup", new Document()
                                .append("from", Product.metaClass.simpleName())
                                .append("localField", "product")
                                .append("foreignField", "_id")
                                .append("as", "product")),
                        new Document("$unwind", new Document()
                                .append("path", "$product")
                                .append("preserveNullAndEmptyArrays", true)),
                        new Document("$lookup", new Document()
                                .append("from", Inventory.metaClass.simpleName())
                                .append("localField", "product.inventory")
                                .append("foreignField", "_id")
                                .append("as", "product.inventory")),
                        new Document("$unwind", new Document()
                                .append("path", "$product.inventory")
                                .append("preserveNullAndEmptyArrays", true)),
                        new Document("$match",
                                new Document("$expr",
                                        new Document("$eq", Arrays.asList("$product.inventory.name", "Inventory 1")))),
                        new Document("$count", "aggregation")
                )))
                .doOnNext(System.out::println)
                .ignoreElements()
                .blockingAwait();
    }

    @Test
    public void testObjectWithReference() {
        MongoCollection<Document> storages = mongoDatabase.getCollection(Storage.metaClass.simpleName());
        MongoCollection<Document> products = mongoDatabase.getCollection(Product.metaClass.simpleName());
        MongoCollection<Document> inventories = mongoDatabase.getCollection(Inventory.metaClass.simpleName());

        await(inventories.insertOne(
                new Document("_id", 1)
                        .append("name", "Inventory 1")));

        await(products.insertOne(
                new Document("_id", 1)
                        .append("name", "Product 2")
                        .append("inventory", 1)));

        await(storages.insertOne(
                new Document("_id", 1)
                        .append("product", 1)));

        Observable.fromPublisher(storages
                .aggregate(Arrays.asList(
                        new Document("$lookup", new Document()
                                .append("from", Product.metaClass.simpleName())
                                .append("localField", "product")
                                .append("foreignField", "_id")
                                .append("as", "product")),
                        new Document("$unwind", new Document()
                                .append("path", "$product")
                                .append("preserveNullAndEmptyArrays", true)),
                        new Document("$lookup", new Document()
                                .append("from", Inventory.metaClass.simpleName())
                                .append("localField", "product.inventory")
                                .append("foreignField", "_id")
                                .append("as", "product.inventory")),
                        new Document("$unwind", new Document()
                                .append("path", "$product.inventory")
                                .append("preserveNullAndEmptyArrays", true)),
                        new Document("$match",
                                new Document("$expr",
                                        new Document("$eq", Arrays.asList("$product.inventory.name", "Inventory 1"))))
                                        )))
                .doOnNext(System.out::println)
                .ignoreElements()
                .blockingAwait();
    }

    @Test @Ignore
    public void testBulkUpdate() {
        MongoCollection<Document> products = mongoDatabase.getCollection("Product");
        Completable.fromPublisher(products.insertMany(Arrays.asList(
                new Document("_id", 1).append("name", "Product 1").append("price", 100),
                new Document("_id", 2).append("name", "Product 2").append("price", 200))))
                .blockingAwait();
        Completable.fromPublisher(products.updateMany(
                new Document(),
                new Document("$set",
                        new Document("name", new Document("$concat", Arrays.asList("$name", " - ", "$price")))),
                new UpdateOptions()))
                .blockingAwait();
        Observable.fromPublisher(products.find())
                .doOnNext(System.out::println)
                .ignoreElements()
                .blockingAwait();
    }

    private void await(Publisher<?> publisher) {
        Completable.fromPublisher(publisher).blockingAwait();
    }
}
