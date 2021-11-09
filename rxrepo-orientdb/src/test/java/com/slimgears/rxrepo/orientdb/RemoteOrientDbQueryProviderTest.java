package com.slimgears.rxrepo.orientdb;

import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.slimgears.rxrepo.query.Repository;
import com.slimgears.util.junit.DockerRules;
import com.slimgears.util.test.containers.ContainerConfig;
import com.slimgears.util.test.containers.WaitPolicy;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class RemoteOrientDbQueryProviderTest extends OrientDbQueryProviderTest {
    private static final String dbUrl = "remote:localhost/db";

    @ClassRule
    public static TestRule dbContainerRule = DockerRules.container(ContainerConfig.builder()
                    .waitPolicy(WaitPolicy.busyWaitSeconds(10, RemoteOrientDbQueryProviderTest::isDbAvailable))
                    .image("orientdb:" + Optional.ofNullable(System.getProperty("orientDbVer")).orElse("3.0.38"))
                    .containerName("orientdb")
                    .environmentPut("ORIENTDB_ROOT_PASSWORD", "root")
                    .commandAdd("/orientdb/bin/server.sh", "-Dstorage.useWAL=false", "-Dtx.useLog=false", "-Dscript.polyglot.useGraal=false")
                    .portsPut(2424, 2424)
                    .portsPut(2480, 2480)
                    .build());

    @BeforeClass
    public static void setUpClass() throws IOException {
        Assume.assumeTrue(isDbAvailable());
        AbstractOrientDbQueryProviderTest.setUpClass();
    }

    private static boolean isDbAvailable() {
        try {
            OrientDB client = new OrientDB(dbUrl, "root", "root", OrientDBConfig.defaultConfig());
            List<String> dbs = client.list();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    protected Repository createRepository(OrientDbRepository.Type dbType) {
        return super.createRepository(dbUrl, dbType);
    }
}
