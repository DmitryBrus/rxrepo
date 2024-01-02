package com.slimgears.rxrepo.mem;

import com.slimgears.rxrepo.query.Repository;
import com.slimgears.rxrepo.test.AbstractRepositoryTest;

public class MemoryQueryProviderTest extends AbstractRepositoryTest {
    @Override
    protected Repository createRepository() {
        return MemoryRepository.create();
    }
}
