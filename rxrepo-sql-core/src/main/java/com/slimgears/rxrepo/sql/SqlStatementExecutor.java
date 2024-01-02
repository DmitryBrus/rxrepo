package com.slimgears.rxrepo.sql;

import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.rxrepo.util.PropertyResolver;
import io.reactivex.Completable;
import io.reactivex.Observable;
import io.reactivex.Single;

import java.util.Arrays;
import java.util.Collections;
import java.util.function.Supplier;

public interface SqlStatementExecutor {
    Observable<PropertyResolver> executeQuery(SqlStatement statement);
    Single<Integer> executeCommandReturnCount(SqlStatement statement);
    Completable executeCommands(Iterable<SqlStatement> statements);
    Observable<Notification<PropertyResolver>> executeLiveQuery(SqlStatement statement);
    Observable<PropertyResolver> executeCommandReturnEntries(SqlStatement statement);

    default SqlStatementExecutor decorate(SqlStatementExecutor.Decorator decorator) {
        return decorator.apply(this);
    }

    default Completable executeCommand(SqlStatement statement) {
        return executeCommands(Collections.singleton(statement));
    }


    interface Decorator {
        SqlStatementExecutor apply(SqlStatementExecutor executor);

        default Decorator andThen(Decorator decorator) {
            return src -> decorator.apply(this.apply(src));
        }

        static Decorator identity() {
            return src -> src;
        }

        static Decorator defer(Supplier<Decorator> supplier) {
            return src -> supplier.get().apply(src);
        }

        static Decorator of(Decorator... decorators) {
            return Arrays.stream(decorators)
                    .reduce(Decorator::andThen)
                    .orElseGet(Decorator::identity);
        }
    }
}
