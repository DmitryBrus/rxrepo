package com.slimgears.rxrepo.sql;

import com.slimgears.util.generic.MoreStrings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.slimgears.util.generic.LazyString.lazy;

public abstract class AbstractSqlStatementExecutor implements SqlStatementExecutor {
    protected final AtomicLong operationCounter = new AtomicLong();
    protected final Logger log;

    protected AbstractSqlStatementExecutor() {
        log = LoggerFactory.getLogger(getClass());
    }

    protected void logStatement(String title, SqlStatement statement) {
        log.trace("[{}] {}: {}", operationCounter.get(), title, lazy(() -> toString(statement)));
    }

    protected String toString(SqlStatement statement) {
        return statement.statement() + "(params: [" +
                IntStream.range(0, statement.args().length)
                        .mapToObj(i -> formatArg(i, statement.args()[i]))
                        .collect(Collectors.joining(", "));
    }

    private String formatArg(int index, Object obj) {
        String type = Optional.ofNullable(obj)
                .map(o -> o.getClass().getSimpleName())
                .orElse("null");
        return MoreStrings.format("#{}[{}]: {}", index, type, obj);
    }
}
