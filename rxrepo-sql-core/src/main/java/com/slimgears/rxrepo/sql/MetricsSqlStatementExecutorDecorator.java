package com.slimgears.rxrepo.sql;

import com.slimgears.nanometer.MetricCollector;
import com.slimgears.rxrepo.query.Notification;
import com.slimgears.rxrepo.util.PropertyResolver;
import io.reactivex.Completable;
import io.reactivex.Observable;
import io.reactivex.Single;

public class MetricsSqlStatementExecutorDecorator implements SqlStatementExecutor.Decorator {
    private final MetricCollector metricCollector;

    private MetricsSqlStatementExecutorDecorator(MetricCollector metricCollector) {
        this.metricCollector = metricCollector.name("sqlExecutor");
    }

    public static SqlStatementExecutor.Decorator create(MetricCollector metricCollector) {
        return new MetricsSqlStatementExecutorDecorator(metricCollector);
    }

    @Override
    public SqlStatementExecutor apply(SqlStatementExecutor executor) {
        return new SqlStatementExecutor() {
            @Override
            public Observable<PropertyResolver> executeQuery(SqlStatement statement) {
                return executor.executeQuery(statement).compose(asyncCollector("query").forObservable());
            }

            @Override
            public Single<Integer> executeCommandReturnCount(SqlStatement statement) {
                return executor.executeCommandReturnCount(statement).compose(asyncCollector("command").forSingle());
            }

            @Override
            public Completable executeCommands(Iterable<SqlStatement> statements) {
                return executor.executeCommands(statements).compose(asyncCollector("command").forCompletable());
            }

            @Override
            public Observable<Notification<PropertyResolver>> executeLiveQuery(SqlStatement statement) {
                return executor.executeLiveQuery(statement).compose(asyncCollector("liveQuery").forObservable());
            }

            @Override
            public Observable<PropertyResolver> executeCommandReturnEntries(SqlStatement statement) {
                return executor.executeCommandReturnEntries(statement).compose(asyncCollector("command").forObservable());
            }

            private MetricCollector.Async asyncCollector(String name) {
                return metricCollector.name(name).asyncDefault();
            }
        };
    }
}
