package com.slimgears.rxrepo.orientdb;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.slimgears.nanometer.MetricCollector;
import com.slimgears.nanometer.Metrics;
import com.slimgears.util.generic.RecurrentThreadLocal;
import com.slimgears.util.stream.Safe;
import io.reactivex.*;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.subjects.CompletableSubject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@SuppressWarnings("ResultOfMethodCallIgnored")
class OrientDbSessionProvider implements AutoCloseable {
    private final static MetricCollector metrics = Metrics.collector(OrientDbRepository.class);
    private final static Logger log = LoggerFactory.getLogger(OrientDbSessionProvider.class);
    private final AtomicInteger currentlyActiveSessions = new AtomicInteger();
    private final MetricCollector.Gauge activeSessionsGauge = metrics.gauge("activeSessions");
    private final ExecutorService executorService;
    private final Maybe<ODatabaseDocument> session;
    private final RecurrentThreadLocal<ODatabaseDocument> sessionThreadLocal;
    private final CompletableSubject closedSubject = CompletableSubject.create();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Object cancellationToken = new Object();

    private OrientDbSessionProvider(Callable<ODatabaseDocument> databaseSessionProvider, int maxConnections) {
        Supplier<ODatabaseDocument> safeSessionProvider = Safe.ofCallable(() -> {
            ODatabaseDocument session = databaseSessionProvider.call();
            session.activateOnCurrentThread();
            return session;
        });

        sessionThreadLocal = RecurrentThreadLocal
                .of(safeSessionProvider)
                .onRelease(Safe.ofConsumer(ODatabaseDocument::close));

        Maybe<ODatabaseDocument> currentSession = Maybe.<ODatabaseDocument>create(emitter -> {
            ODatabaseDocument s = sessionThreadLocal.acquire();
            try {
                if (closed.get()) {
                    emitter.onComplete();
                } else {
                    emitter.onSuccess(s);
                }
            } finally {
                sessionThreadLocal.release();
            }
        }).doOnSubscribe(d -> {
            int newCount = currentlyActiveSessions.incrementAndGet();
            log.trace("Created database connection (currently active connections: {})", newCount);
            activeSessionsGauge.record(newCount);
        }).doFinally(() -> {
            int newCount = currentlyActiveSessions.decrementAndGet();
            log.trace("Database connection closed (currently active connections: {}", newCount);
            activeSessionsGauge.record(newCount);
        });

        this.executorService = Executors.newWorkStealingPool(maxConnections);
        Scheduler scheduler = Schedulers.from(executorService);
        this.session = currentSession.subscribeOn(scheduler);
    }

    static OrientDbSessionProvider create(Callable<ODatabaseDocument> dbSessionSupplier, int maxConnections) {
        return new OrientDbSessionProvider(dbSessionSupplier, maxConnections);
    }

    Maybe<ODatabaseDocument> session() {
        return session.takeUntil(closedSubject.andThen(Maybe.just(cancellationToken)));
    }

    synchronized void withSession(Consumer<ODatabaseDocument> action) {
        if (!closed.get()) {
            this.getWithSession(session -> {
                action.accept(session);
                return null;
            });
        }
    }

    <T> T getWithSession(Function<ODatabaseDocument, T> func) {
        ODatabaseDocument session = sessionThreadLocal.acquire();
        try {
            return func.apply(session);
        } finally {
            sessionThreadLocal.release();
        }
    }

    Completable completeWithSession(io.reactivex.functions.Consumer<ODatabaseDocument> action) {
        return session().doOnSuccess(action).ignoreElement();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            closedSubject.onComplete();
            //this.executorService.shutdown();
            this.executorService.shutdownNow();
            try {
                this.executorService.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            log.debug("Active threads: {}", Thread.activeCount());
        }
    }
}
