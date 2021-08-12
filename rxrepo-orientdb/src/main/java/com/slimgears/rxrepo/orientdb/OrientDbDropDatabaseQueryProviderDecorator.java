package com.slimgears.rxrepo.orientdb;

import com.orientechnologies.orient.core.db.OrientDB;
import com.slimgears.rxrepo.query.decorator.AbstractQueryProviderDecorator;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import io.reactivex.Completable;
import io.reactivex.schedulers.Schedulers;

import java.util.function.Supplier;

@SuppressWarnings("WeakerAccess")
class OrientDbDropDatabaseQueryProviderDecorator extends AbstractQueryProviderDecorator {
    private final Supplier<OrientDB> clientSupplier;
    private final String dbName;

    private OrientDbDropDatabaseQueryProviderDecorator(QueryProvider underlyingProvider, Supplier<OrientDB> clientSupplier, String dbName) {
        super(underlyingProvider);
        this.clientSupplier = clientSupplier;
        this.dbName = dbName;
    }

    public static QueryProvider.Decorator create(Supplier<OrientDB> clientSupplier, String dbName) {
        return queryProvider -> new OrientDbDropDatabaseQueryProviderDecorator(queryProvider, clientSupplier, dbName);
    }

    @Override
    public Completable dropAll() {
        return Completable
                .fromAction(() -> {
                    OrientDB client = clientSupplier.get();
                    if (client.exists(dbName)) {
                        client.drop(dbName);
                    }
                })
                .subscribeOn(Schedulers.io());
    }
}
