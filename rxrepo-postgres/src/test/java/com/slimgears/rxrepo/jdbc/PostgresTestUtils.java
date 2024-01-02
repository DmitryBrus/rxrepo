package com.slimgears.rxrepo.jdbc;

import com.slimgears.util.junit.ResourceRules;
import com.slimgears.util.test.containers.ContainerConfig;
import com.slimgears.util.test.containers.DockerUtils;
import com.slimgears.util.test.containers.WaitPolicy;
import org.junit.Ignore;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

@Ignore
public class PostgresTestUtils {
    public static final String connectionUrl = "jdbc:postgresql://localhost/test_db?user=root&password=root";
    public static final String schemaName = "repository";

    public static TestRule rule() {
        ContainerConfig containerConfig = ContainerConfig.builder()
                .image("postgres:13.4")
                .containerName("postgres")
                .environmentPut("POSTGRES_DB", "test_db")
                .environmentPut("POSTGRES_USER", "root")
                .environmentPut("POSTGRES_PASSWORD", "root")
                .portsPut(5432, 5432)
                .waitPolicy(WaitPolicy.delaySeconds(8))
                .build();

        return ResourceRules.toRule(() -> {
            AutoCloseable container = System.getProperty("test.useCompose") != null
                    ? DockerUtils.withCompose("docker-compose.yaml")
                    : DockerUtils.withContainer(containerConfig);
            try {
                Connection connection = DriverManager.getConnection(connectionUrl);
                connection
                        .prepareStatement("DROP SCHEMA IF EXISTS " + schemaName + " CASCADE")
                        .execute();
            } catch (SQLException e) {
                try {
                    container.close();
                } catch (Exception ignored) {
                }
                throw new RuntimeException(e);
            }
            return container;
        });
    }
}
