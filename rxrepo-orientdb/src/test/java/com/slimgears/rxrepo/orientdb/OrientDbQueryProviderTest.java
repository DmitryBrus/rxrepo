package com.slimgears.rxrepo.orientdb;

import com.slimgears.rxrepo.query.Repository;

public class OrientDbQueryProviderTest extends AbstractOrientDbQueryProviderTest {
    private static final String dbUrl = "embedded:db";

    @Override
    protected Repository createRepository() {
        return createRepository(OrientDbRepository.Type.Memory);
    }

    protected Repository createRepository(OrientDbRepository.Type dbType) {
        return super.createRepository(dbUrl, dbType);
    }
}
