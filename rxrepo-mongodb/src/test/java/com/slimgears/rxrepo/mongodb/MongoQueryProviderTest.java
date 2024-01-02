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
public class MongoQueryProviderTest extends AbstractRepositoryTest {
    @ClassRule
    public static TestRule containerRule = MongoTestUtils.rule();

    @Override
    protected Repository createRepository() {
        return MongoRepository.builder()
                .port(MongoTestUtils.port)
                .maxConcurrentRequests(100)
                .decorate(SubscribeOnSchedulingQueryProviderDecorator.createDefault())
                .build();
    }
}
