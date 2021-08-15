package com.slimgears.rxrepo.mongodb;

import com.mongodb.ConnectionString;
import com.slimgears.rxrepo.mongodb.embed.MongoService;
import com.slimgears.util.generic.MoreStrings;
import com.slimgears.util.junit.DockerRules;
import com.slimgears.util.junit.ResourceRules;
import org.junit.rules.TestRule;

import java.math.BigInteger;
import java.util.Random;

public class MongoTestUtils {
    final static int port = 27018;
    final static ConnectionString connectionString = new ConnectionString("mongodb://localhost:" + port);

    public static TestRule rule() {
        return ResourceRules.toRule(MongoTestUtils::startMongo);
    }

    private static AutoCloseable startMongo() {
        return MongoService.builder()
                .port(port)
                .version("4.0.12")
                .enableReplica()
                .build()
                .start();
    }
}
