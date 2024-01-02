package com.slimgears.rxrepo.jdbc;

import com.slimgears.rxrepo.postgres.PostgresRepository;
import com.slimgears.rxrepo.postgres.PostgresSqlStatementProvider;
import com.slimgears.rxrepo.query.Repository;
import com.slimgears.rxrepo.sql.*;
import com.slimgears.rxrepo.sql.jdbc.JdbcHelper;
import com.slimgears.rxrepo.test.*;
import com.slimgears.util.junit.AnnotationRulesJUnit;
import com.slimgears.util.junit.DockerRules;
import com.slimgears.util.test.logging.LogLevel;
import com.slimgears.util.test.logging.UseLogLevel;
import org.junit.*;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;

import java.sql.*;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@Ignore
@RunWith(AnnotationRulesJUnit.class)
public class PostgresqlRawTest {
    @ClassRule public static TestRule dbContainerRule = PostgresTestUtils.rule();
    private SqlStatementProvider statementProvider;
    private Callable<Connection> connection;

    @Before
    public void setUp() {
        SqlTypeMapper sqlTypeMapper = SqlTypes.instance;
        SqlExpressionGenerator expressionGenerator = new DefaultSqlExpressionGenerator();
        statementProvider = new PostgresSqlStatementProvider(expressionGenerator, sqlTypeMapper, () -> "repository");
        connection = () -> DriverManager.getConnection("jdbc:postgresql://localhost/test_db?user=root&password=root");
    }

    @Test
    public void testLargeUpdate() throws Exception {
        Connection connection = this.connection.call();
        connection.prepareStatement("CREATE TABLE IF NOT EXISTS test_table (id_id INTEGER , id_areaId INTEGER, id_type VARCHAR, idAsString VARCHAR(16), name VARCHAR);").execute();
        int count = 100000;
        insertEntries(connection, IntStream.range(1, count + 1)
                .mapToObj(UniqueId::manufacturerId)
                .map(id -> Manufacturer.create(id, "Manufacturer" + id.id())));
        ResultSet result = connection.prepareStatement("SELECT COUNT(*) FROM test_table").executeQuery();
        while (result.next()) {
            System.out.println("Query result: " + result.getInt(1));
        }
    }

    @Test
    @UseLogLevel(LogLevel.INFO)
    public void testRepository() {
        int count = 100000;
        Repository repository = PostgresRepository
                .builder()
                .connection(connection)
                .enableBatch(1000)
                .build();
        repository.entities(Product.metaClass)
                .update(Products.createMany(count))
                .blockingAwait();
        Assert.assertEquals(Long.valueOf(count), repository.entities(Product.metaClass).query().count().blockingGet());
    }

    @Test
    public void testCreateSchema() throws Exception {
        execute(statementProvider.forCreateSchema());
        execute(statementProvider.forCreateTable(Manufacturer.metaClass));
        execute(statementProvider.forCreateTable(Inventory.metaClass));
        execute(statementProvider.forCreateTable(Product.metaClass));
        execute(statementProvider.forDropSchema());
    }

    private void execute(SqlStatement statement) throws Exception {
        System.out.println("Executing:\n" + statement.statement() + "\n");
        System.out.println("Args: " + Arrays.stream(statement.args()).map(Object::toString).collect(Collectors.joining(", ", "[", "]")));
        PreparedStatement preparedStatement = JdbcHelper.prepareStatement(connection.call(), statement);
        preparedStatement.execute();
    }

    private void insertEntries(Connection connection, Stream<Manufacturer> entries) {
        KeyEncoder keyEncoder = DigestKeyEncoder.create("SHA1", 16);
        StringBuilder insertStatement = new StringBuilder();
        insertStatement.append("INSERT INTO test_table (id_id, id_areaId, id_type, idAsString, name) VALUES ");
        insertStatement.append(entries
                        .map(m -> Stream.of(
                                String.valueOf(m.id().id()),
                                String.valueOf(m.id().areaId()),
                                "'" + m.id().type() + "'",
                                "'" + keyEncoder.encode(m.id()) + "'",
                                "'" + m.name() + "'").collect(Collectors.joining(", ", "(", ")")))
                        .collect(Collectors.joining(",")));
        try {
            long rows = connection.prepareStatement(insertStatement.toString()).executeLargeUpdate();
            System.out.println("Result: " + rows);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
