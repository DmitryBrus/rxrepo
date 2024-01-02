package com.slimgears.rxrepo.sql;

import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.util.PropertyResolver;
import com.slimgears.util.stream.Streams;
import io.reactivex.Completable;
import io.reactivex.Observable;
import io.reactivex.Single;

import java.util.stream.Collectors;

public abstract class AbstractSqlStatementExecutorDecorator implements SqlStatementExecutor {
    protected final SqlStatementExecutor underlyingExecutor;

    protected AbstractSqlStatementExecutorDecorator(SqlStatementExecutor underlyingExecutor) {
        this.underlyingExecutor = underlyingExecutor;
    }

    @Override
    public Observable<PropertyResolver> executeQuery(SqlStatement statement) {
        return underlyingExecutor.executeQuery(statement.mapArgs(this::mapArgument));
    }

    @Override
    public Observable<PropertyResolver> executeCommandReturnEntries(SqlStatement statement) {
        return underlyingExecutor.executeCommandReturnEntries(mapArgs(statement));
    }

    @Override
    public Single<Integer> executeCommandReturnCount(SqlStatement statement) {
        return underlyingExecutor.executeCommandReturnCount(statement.mapArgs(this::mapArgument));
    }

    @Override
    public Completable executeCommand(SqlStatement statement) {
        return underlyingExecutor.executeCommand(statement.mapArgs(this::mapArgument));
    }

    @Override
    public Completable executeCommands(Iterable<SqlStatement> statements) {
        return underlyingExecutor.executeCommands(
                Streams.fromIterable(statements).map(this::mapArgs).collect(Collectors.toList()));
    }

    @Override
    public Observable<Notification<PropertyResolver>> executeLiveQuery(SqlStatement statement) {
        return underlyingExecutor.executeLiveQuery(statement.mapArgs(this::mapArgument));
    }

    protected Object mapArgument(Object arg) {
        return arg;
    }

    private SqlStatement mapArgs(SqlStatement statement) {
        return statement.mapArgs(this::mapArgument);
    }
}
