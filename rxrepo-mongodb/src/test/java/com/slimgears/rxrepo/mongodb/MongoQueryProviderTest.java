package com.slimgears.rxrepo.mongodb;

import com.slimgears.rxrepo.query.Repository;
import com.slimgears.rxrepo.query.decorator.SubscribeOnSchedulingQueryProviderDecorator;
import com.slimgears.rxrepo.test.AbstractRepositoryTest;
import com.slimgears.util.junit.DockerRules;
import com.slimgears.util.test.logging.LogLevel;
import com.slimgears.util.test.logging.UseLogLevel;
import com.slimgears.util.test.logging.UseLogLevels;
import org.junit.*;
import org.junit.rules.TestRule;

@UseLogLevels(
        @UseLogLevel(logger = "org.mongodb.driver", value = LogLevel.INFO)
)
@Ignore
public class MongoQueryProviderTest extends AbstractRepositoryTest {
    private final static String mongoUser = "root";
    private final static String mongoPassword = "example";

    @ClassRule
    public static TestRule containerRule = DockerRules.compose();

    @Override
    protected Repository createRepository() {
        return MongoRepository.builder()
                .port(MongoTestUtils.port)
                .maxConcurrentRequests(100)
                .user(mongoUser)
                .password(mongoPassword)
                .decorate(SubscribeOnSchedulingQueryProviderDecorator.createDefault())
                .build();
    }
}
