package com.slimgears.rxrepo.orientdb;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.serialization.types.OBinaryTypeSerializer;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.index.ORuntimeKeyIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.sequence.OSequence;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.slimgears.rxrepo.test.UniqueId;
import com.slimgears.util.junit.AnnotationRulesJUnit;
import io.reactivex.Observable;
import io.reactivex.functions.Consumer;
import io.reactivex.observers.TestObserver;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.MethodRule;

public class OrientDbClientTest {
    @Rule public final MethodRule annotationRules = AnnotationRulesJUnit.rule();
    private static final String dbUrl = "embedded:db";
    private final static String dbName = "testDb";

    @Test
    public void orientDbClientTest() throws Exception {
        rawDbTest(session -> {
            session.createClassIfNotExist("MyClass");
            session.command("insert into MyClass set `name`=?", "test")
                    .stream()
                    .forEach(System.out::println);

            session.command("select count() from MyClass")
                    .stream()
                    .forEach(System.out::println);
        });
    }

    private OrientDB createClient() {
        OrientDB db = new OrientDB(dbUrl, OrientDBConfig.defaultConfig());
        db.createIfNotExists(dbName, ODatabaseType.MEMORY);
        return db;
    }

    private void rawDbTest(Consumer<ODatabaseSession> test) throws Exception {
        try (OrientDB dbClient = createClient()) {
            try (ODatabaseSession dbSession = dbClient.open(dbName, "admin", "admin")) {
                test.accept(dbSession);
            } finally {
                dbClient.drop(dbName);
            }
        }
    }

    @Test
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

    @Test
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

    @Test
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
}
