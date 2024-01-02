package com.slimgears.rxrepo.sql.jdbc;

import com.slimgears.rxrepo.sql.SqlStatement;
import io.reactivex.Observable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.Callable;

public class JdbcHelper {
    public static PreparedStatement prepareStatement(Connection connection, SqlStatement statement) {
        return prepareStatement(() -> connection.prepareStatement(statement.statement()), statement.args());
    }

    public static PreparedStatement prepareStatement(Callable<PreparedStatement> supplier, Object[] params) {
        try {
            PreparedStatement preparedStatement = supplier.call();
            setParams(preparedStatement, params);
            return preparedStatement;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void setParams(PreparedStatement preparedStatement, Object[] params) throws SQLException {
        for (int i = 0; i < params.length; ++i) {
            preparedStatement.setObject(i + 1, params[i]);
        }
    }

    public static Observable<ResultSet> toObservable(ResultSet resultSet) {
        return Observable.<ResultSet>generate(emitter -> {
            if (resultSet.next()) {
                emitter.onNext(resultSet);
            } else {
                emitter.onComplete();
            }
        }).doFinally(resultSet::close);
    }
}
