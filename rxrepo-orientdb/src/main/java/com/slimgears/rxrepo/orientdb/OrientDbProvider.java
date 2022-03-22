package com.slimgears.rxrepo.orientdb;

import com.orientechnologies.orient.core.db.OrientDB;

@FunctionalInterface
public interface OrientDbProvider {
    OrientDB acquire();

    default void release(OrientDB orientDB) {
        orientDB.close();
    }

    default OrientDbProvider refCount() {
        return new RefCounterOrientDbProvider(this);
    }
}
