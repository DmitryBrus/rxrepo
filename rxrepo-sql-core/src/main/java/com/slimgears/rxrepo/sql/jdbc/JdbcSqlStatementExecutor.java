package com.slimgears.rxrepo.sql.jdbc;

import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.sql.AbstractSqlStatementExecutor;
import com.slimgears.rxrepo.sql.SqlStatement;
import com.slimgears.rxrepo.sql.SqlTypeMapper;
import com.slimgears.rxrepo.util.PropertyResolver;
import com.slimgears.util.stream.Safe;
import com.slimgears.util.stream.Streams;
import io.reactivex.Completable;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.reactivex.disposables.Disposable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

public class JdbcSqlStatementExecutor extends AbstractSqlStatementExecutor {
    private final Callable<Connection> connectionSupplier;
    private final SqlTypeMapper typeMapper;

    public JdbcSqlStatementExecutor(Callable<Connection> connectionSupplier,
                                    SqlTypeMapper typeMapper) {
        this.connectionSupplier = connectionSupplier;
        this.typeMapper = typeMapper;
    }

    @Override
    public Observable<PropertyResolver> executeQuery(SqlStatement statement) {
        return Observable.defer(() -> {
            try (Connection connection = connectionSupplier.call()) {
                PreparedStatement preparedStatement = JdbcHelper.prepareStatement(
                        connection,
                        statement);
                logStatement("Executing query", statement);
                ResultSet resultSet = preparedStatement.executeQuery();
                return JdbcHelper.toObservable(resultSet)
                        .map(rs -> JdbcResultSetPropertyResolver.create(rs, typeMapper))
                        .onErrorResumeNext((Throwable e) -> Observable.error(mapException(e)));
            }
        });
    }

    @Override
    public Observable<PropertyResolver> executeCommandReturnEntries(SqlStatement statement) {
        return Observable.create(emitter -> {
            try (Connection connection = connectionSupplier.call()) {
                PreparedStatement ps = JdbcHelper.prepareStatement(connection, statement);
                try (ResultSet rs = ps.executeQuery()) {
                    Disposable disposable = JdbcHelper.toObservable(rs)
                            .map(_rs -> JdbcResultSetPropertyResolver.create(_rs, typeMapper))
                            .subscribe(emitter::onNext, emitter::onError, emitter::onComplete);
                    emitter.setDisposable(disposable);
                } catch (SQLException e) {
                    emitter.onError(mapException(e));
                }
            }
        });
    }

    @Override
    public Single<Integer> executeCommandReturnCount(SqlStatement statement) {
        return Single.<Integer>create(emitter -> {
            try (Connection connection = connectionSupplier.call()) {
                PreparedStatement preparedStatement = JdbcHelper.prepareStatement(
                        connection,
                        statement);
                logStatement("Executing update", statement);
                emitter.onSuccess((int)preparedStatement.executeLargeUpdate());
            }
        }).onErrorResumeNext(e -> Single.error(mapException(e)));
    }

    @Override
    public Completable executeCommands(Iterable<SqlStatement> statements) {
        return Completable.create(emitter -> {
            try (Connection connection = connectionSupplier.call()) {
                connection.setAutoCommit(false);
                Collection<PreparedStatement> preparedStatements = Streams.fromIterable(statements)
                        .peek(s -> logStatement("Executing command", s))
                        .map(s -> JdbcHelper.prepareStatement(connection, s))
                        .peek(Safe.ofConsumer(PreparedStatement::execute))
                        .collect(Collectors.toList());
                try {
                    connection.commit();
                    emitter.onComplete();
                } finally {
                    preparedStatements.forEach(Safe.ofConsumer(PreparedStatement::close));
                }
            }
        }).onErrorResumeNext(e -> Completable.error(mapException(e)));
    }

    @Override
    public Observable<Notification<PropertyResolver>> executeLiveQuery(SqlStatement statement) {
        return notImplemented();
    }

    private Throwable mapException(Throwable e) {
        if (e.getMessage().contains("duplicate")) {
            return new ConcurrentModificationException(e);
        }
        return e;
    }

    private static <T> T notImplemented() {
        throw new UnsupportedOperationException("Not implemented yet");
    }
}
