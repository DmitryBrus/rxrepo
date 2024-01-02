package com.slimgears.rxrepo.orientdb;

import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.sql.KeyEncoder;
import com.slimgears.rxrepo.sql.SqlStatement;
import com.slimgears.rxrepo.sql.SqlStatementExecutor;
import com.slimgears.rxrepo.util.PropertyResolver;
import com.slimgears.util.stream.Streams;
import io.reactivex.Completable;
import io.reactivex.Observable;
import io.reactivex.Single;

import java.util.stream.Collectors;

public class OrientDbMappingStatementExecutor implements SqlStatementExecutor {
    private final SqlStatementExecutor underlyingExecutor;
    private final OrientDbObjectConverter objectConverter;

    private OrientDbMappingStatementExecutor(SqlStatementExecutor underlyingExecutor, OrientDbObjectConverter objectConverter) {
        this.underlyingExecutor = underlyingExecutor;
        this.objectConverter = objectConverter;
    }

    public static class Decorator implements SqlStatementExecutor.Decorator {
        private final KeyEncoder keyEncoder;

        private Decorator(KeyEncoder keyEncoder) {
            this.keyEncoder = keyEncoder;
        }

        public static Decorator create(KeyEncoder keyEncoder) {
            return new Decorator(keyEncoder);
        }

        @Override
        public SqlStatementExecutor apply(SqlStatementExecutor executor) {
            return OrientDbMappingStatementExecutor.decorate(executor, keyEncoder);
        }
    }

    static SqlStatementExecutor decorate(SqlStatementExecutor executor, KeyEncoder keyEncoder) {
        return new OrientDbMappingStatementExecutor(executor, OrientDbObjectConverter.create(keyEncoder));
    }

    @Override
    public Observable<PropertyResolver> executeQuery(SqlStatement statement) {
        return underlyingExecutor.executeQuery(toOrientDb(statement));
    }

    @Override
    public Observable<PropertyResolver> executeCommandReturnEntries(SqlStatement statement) {
        return underlyingExecutor.executeCommandReturnEntries(toOrientDb(statement));
    }

    private SqlStatement toOrientDb(SqlStatement statement) {
        return statement.mapArgs(objectConverter::toOrientDbObject);
    }

    @Override
    public Single<Integer> executeCommandReturnCount(SqlStatement statement) {
        return underlyingExecutor.executeCommandReturnCount(toOrientDb(statement));
    }

    @Override
    public Completable executeCommand(SqlStatement statement) {
        return underlyingExecutor.executeCommand(toOrientDb(statement));
    }

    @Override
    public Completable executeCommands(Iterable<SqlStatement> statements) {
        return underlyingExecutor.executeCommands(Streams.fromIterable(statements).map(this::toOrientDb).collect(Collectors.toList()));
    }

    @Override
    public Observable<Notification<PropertyResolver>> executeLiveQuery(SqlStatement statement) {
        return underlyingExecutor.executeLiveQuery(toOrientDb(statement));
    }
}
