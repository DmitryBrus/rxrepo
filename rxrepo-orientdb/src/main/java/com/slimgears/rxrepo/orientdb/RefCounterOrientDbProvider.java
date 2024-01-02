package com.slimgears.rxrepo.orientdb;

import com.orientechnologies.orient.core.db.OrientDB;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class RefCounterOrientDbProvider implements OrientDbProvider {
    private final AtomicInteger refCounter = new AtomicInteger();
    private final AtomicReference<OrientDB> instance = new AtomicReference<>();
    private final OrientDbProvider orientDbProvider;

    public RefCounterOrientDbProvider(OrientDbProvider orientDbProvider) {
        this.orientDbProvider = orientDbProvider;
    }

    @Override
    public OrientDB acquire() {
        if (refCounter.getAndIncrement() == 0) {
            synchronized (instance) {
                instance.set(orientDbProvider.acquire());
            }
        }
        return instance.get();
    }

    @Override
    public void release(OrientDB orientDB) {
        if (refCounter.decrementAndGet() == 0) {
            synchronized (instance) {
                Optional.ofNullable(instance.get())
                        .ifPresent(i -> {
                            orientDbProvider.release(i);
                            instance.set(null);
                        });
            }
        }
    }
}
