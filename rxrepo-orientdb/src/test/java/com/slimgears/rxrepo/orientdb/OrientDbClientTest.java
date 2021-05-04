package com.slimgears.rxrepo.orientdb;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.serialization.types.OBinaryTypeSerializer;
import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.index.ORuntimeKeyIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.sequence.OSequence;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.slimgears.rxrepo.sql.CacheSchemaProviderDecorator;
import com.slimgears.rxrepo.sql.SchemaProvider;
import com.slimgears.rxrepo.test.Inventory;
import com.slimgears.rxrepo.test.Product;
import com.slimgears.rxrepo.test.UniqueId;
import com.slimgears.util.junit.AnnotationRulesJUnit;
import com.slimgears.util.test.logging.LogLevel;
import com.slimgears.util.test.logging.UseLogLevel;
import io.reactivex.Observable;
import io.reactivex.functions.Consumer;
import io.reactivex.observers.TestObserver;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.MethodRule;

import java.util.function.Supplier;

public class OrientDbClientTest {
    @Rule public final MethodRule annotationRules = AnnotationRulesJUnit.rule();
    private static final String dbUrl = "embedded:db";
    private final static String dbName = "testDb";

    @Test
    public void orientDbClientTest() {
        OrientDB client = createClient(dbName);
        client.close();

        client = createClient(dbName);
        client.close();
    }

    private OrientDB createClient(String dbName) {
        OrientDB db = new OrientDB(dbUrl, OrientDBConfig.defaultConfig());
        db.createIfNotExists(dbName, ODatabaseType.PLOCAL);
        ODatabaseDocument session = db.open(dbName, "admin", "admin");
        session.createClassIfNotExist("MyClass");
        session.command("insert into MyClass set `name`=?", "test")
                .stream()
                .forEach(System.out::println);

        session.command("select count() from MyClass")
                .stream()
                .forEach(System.out::println);

        session.close();
        return db;
    }

    private void rawDbTest(Consumer<ODatabaseSession> test) throws Exception {
        try (OrientDB dbClient = new OrientDB(dbUrl, OrientDBConfig.defaultConfig())) {
            dbClient.create(dbName, ODatabaseType.MEMORY);
            try (ODatabaseSession dbSession = dbClient.open(dbName, "admin", "admin")) {
                test.accept(dbSession);
            }
        }
    }

    @Test @Ignore
    public void testQueryByObject() throws Exception {
        rawDbTest(dbSession -> {
            OClass oClass = dbSession.createClass("MyClass");
            oClass.createProperty("myField", OType.EMBEDDED);
            OElement element = dbSession.newElement();
            element.setProperty("id", 1);
            element.setProperty("test", "testVal");
            dbSession.command("insert into MyClass set myField = ?", element)
                    .stream()
                    .map(OResult::toJSON)
                    .forEach(el -> System.out.println("Inserted element: " + el));
            dbSession.query("select from MyClass where myField like " + element.toJSON())
                    .stream()
                    .map(OResult::toJSON)
                    .forEach(el -> System.out.println("Received element: " + el));
        });
    }

    @Test @Ignore
    public void testAddThenQueryRawOrientDb() throws Exception {
        rawDbTest(dbSession -> {
            OClass oClass = dbSession.createClass("MyClass");
            oClass.createProperty("myField", OType.EMBEDDED);

            dbSession.command("insert into MyClass set myField = {'id': 1, '@type': 'd'}")
                    .stream()
                    .map(OResult::toJSON)
                    .forEach(el -> System.out.println("Inserted element: " + el));

            dbSession.command("insert into MyClass set myField = ?",
                    new ODocument().field("id", 1))
                    .stream()
                    .map(OResult::toJSON)
                    .forEach(el -> System.out.println("Inserted element: " + el));

            dbSession.query("select from MyClass where myField = {'id': 1, '@type': 'd', '@version': 0}")
                    .stream()
                    .map(OResult::toJSON)
                    .forEach(el -> System.out.println("Retrieved element (with filter): " + el));

            dbSession.query("select from MyClass where myField = ?",
                    new ODocument().field("id", 1))
                    .stream()
                    .map(OResult::toJSON)
                    .forEach(el -> System.out.println("Retrieved element (with filter): " + el));
        });
    }

    @Test @Ignore
    public void testCustomIndex() throws Exception {
        rawDbTest(session -> {
            OClass oClass = session.createClass("MyClass");
            oClass.createProperty("key", OType.STRING);
            session.getMetadata().getIndexManager().createIndex(
                    "MyClass.keyIndex",
                    OClass.INDEX_TYPE.UNIQUE_HASH_INDEX.name(),
                    new ORuntimeKeyIndexDefinition<>(OBinaryTypeSerializer.ID),
                    null,
                    null,
                    null);
            session.command("update MyClass set key = ? upsert return after where (key = ?)", UniqueId.productId(2), UniqueId.productId(2))
                    .close();

            session.command("update MyClass set key = ? upsert return after where (key = ?)", UniqueId.productId(2), UniqueId.productId(2))
                    .stream()
                    .close();

            session.query("select from MyClass where key = ?", UniqueId.productId(2))
                    .stream()
                    .map(OResult::toJSON)
                    .forEach(System.out::println);
        });
    }

    @Test
    public void testOrientDbSchemeProvider() throws InterruptedException {
        OrientDB dbClient = new OrientDB(dbUrl, OrientDBConfig.defaultConfig());
        dbClient.createIfNotExists(dbName, ODatabaseType.MEMORY);
        try {
            ODatabasePool oDatabasePool = new ODatabasePool(dbClient, dbName, "admin", "admin");
            Supplier<ODatabaseDocument> dbSessionSupplier = oDatabasePool::acquire;
            SchemaProvider schemaProvider = new OrientDbSchemaProvider(OrientDbSessionProvider.create(dbSessionSupplier));
            SchemaProvider cachedSchemaProvider = CacheSchemaProviderDecorator.decorate(schemaProvider);
            cachedSchemaProvider.createOrUpdate(Inventory.metaClass)
                    .test()
                    .await()
                    .assertNoErrors()
                    .assertComplete();
        } finally {
            dbClient.drop(dbName);
        }
    }

    @Test
    public void insertWithAutoIncrement() throws Exception {
        rawDbTest(session -> {
            OClass oClass = session.createClass("MyClass");
            OSequence sequence = session.getMetadata().getSequenceLibrary().createSequence("sequence", OSequence.SEQUENCE_TYPE.ORDERED, new OSequence.CreateParams());
            oClass.createProperty("num", OType.LONG);
            Observable<OElement> elements = Observable.create(emitter -> session.live("select from MyClass", new OLiveQueryResultListener() {
                @Override
                public void onCreate(ODatabaseDocument database, OResult data) {
                    emitter.onNext(data.toElement());
                }

                @Override
                public void onUpdate(ODatabaseDocument database, OResult before, OResult after) {
                }

                @Override
                public void onDelete(ODatabaseDocument database, OResult data) {

                }

                @Override
                public void onError(ODatabaseDocument database, OException exception) {
                    emitter.onError(exception);
                }

                @Override
                public void onEnd(ODatabaseDocument database) {
                    emitter.onComplete();
                }
            }));
            TestObserver<OElement> testObserver = elements
                    .doOnNext(System.out::println)
                    .test();
            session.begin();
            OElement element = session.newElement("MyClass");
            element.setProperty("num", sequence.next());
            element.save();
            session.commit();
            testObserver.awaitCount(1)
                    .assertValueCount(1)
                    .assertValueAt(0, e -> e.getProperty("num").equals(1L));
        });
    }

    @Test
    @UseLogLevel(LogLevel.TRACE)
    public void insertWithAutoIncrementWithProvider() {
        OrientDB dbClient = new OrientDB(dbUrl, OrientDBConfig.defaultConfig());
        dbClient.createIfNotExists(dbName, ODatabaseType.MEMORY);
        OrientDbObjectConverter converter = OrientDbObjectConverter.create(
                metaClass -> new ODocument(metaClass.simpleName()),
                ((c, entity) -> (OElement) c.toOrientDbObject(entity)));
        OrientDbSessionProvider sessionProvider = OrientDbSessionProvider.create(() -> dbClient.open(dbName, "admin", "admin"));
        OrientDbSchemaProvider schemaProvider = new OrientDbSchemaProvider(sessionProvider);
        schemaProvider.createOrUpdate(Inventory.metaClass)
                .andThen(schemaProvider.createOrUpdate(Product.metaClass))
                .blockingAwait();

        Observable<Long> elements = Observable
                .<OrientDbLiveQueryListener.LiveQueryNotification>create(emitter -> sessionProvider.withSession(session -> {
                    session.live("select from Inventory", new OrientDbLiveQueryListener(emitter, null));
                }))
                .map(OrientDbLiveQueryListener.LiveQueryNotification::sequenceNumber);

        TestObserver<Long> testObserver = elements.doOnNext(System.out::println)
                .test();

        sessionProvider.withSession(session -> {
            session.begin();
            OSequence sequence = session.getMetadata().getSequenceLibrary().getSequence("sequenceNum");
            OElement inventoryElement = (OElement)converter.toOrientDbObject(Inventory
                    .builder()
                    .id(UniqueId.inventoryId(1))
                    .name("Inventory 1")
                    .build());

            inventoryElement.setProperty("__sequenceNum", sequence.next());
            inventoryElement.save();
            session.commit();
        });

        testObserver.awaitCount(1)
                .assertValueCount(1)
                .assertValueAt(0, 1L);
    }
}
