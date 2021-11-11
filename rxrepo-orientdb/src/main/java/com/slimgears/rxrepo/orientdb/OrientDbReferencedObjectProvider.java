package com.slimgears.rxrepo.orientdb;

import com.google.common.cache.*;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.OLiveQueryMonitor;
import com.orientechnologies.orient.core.db.OLiveQueryResultListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.slimgears.rxrepo.util.PropertyResolver;
import com.slimgears.util.generic.MoreStrings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

public class OrientDbReferencedObjectProvider {
    private final static Logger log = LoggerFactory.getLogger(OrientDbReferencedObjectProvider.class);
    private final LoadingCache<ORID, Optional<PropertyResolver>> propertyResolverCache;

    private final OrientDbSessionProvider querySessionProvider;
    private final Map<String, Listener> listenerByClassMap = new ConcurrentHashMap<>();
    private final Map<ORID, Listener> listenerByORID = new ConcurrentHashMap<>();

    class Listener implements OLiveQueryResultListener {
        private final AtomicInteger refCounter = new AtomicInteger();
        private final OLiveQueryMonitor monitor;
        private final String className;

        public Listener(String className) {
            monitor = querySessionProvider.getWithSession(s -> s.live("select from " + className, this));
            this.className = className;
        }

        public Listener acquire() {
            refCounter.incrementAndGet();
            return this;
        }

        public void release() {
            if (refCounter.decrementAndGet() == 0) {
                monitor.unSubscribe();
                listenerByClassMap.remove(className);
            }
        }

        @Override
        public void onCreate(ODatabaseDocument database, OResult data) {

        }

        @Override
        public void onUpdate(ODatabaseDocument database, OResult before, OResult after) {
            after.getIdentity().ifPresent(propertyResolverCache::invalidate);
        }

        @Override
        public void onDelete(ODatabaseDocument database, OResult data) {
            data.getIdentity().ifPresent(propertyResolverCache::invalidate);
        }

        @Override
        public void onError(ODatabaseDocument database, OException exception) {

        }

        @Override
        public void onEnd(ODatabaseDocument database) {

        }
    }

    private OrientDbReferencedObjectProvider(OrientDbSessionProvider querySessionProvider, Duration cacheExpirationTime) {
        this.querySessionProvider = querySessionProvider;
        this.propertyResolverCache = CacheBuilder
                .newBuilder()
                .expireAfterAccess(cacheExpirationTime)
                .concurrencyLevel(10)
                .removalListener((RemovalListener<ORID, Optional<PropertyResolver>>) notification -> removeListener(notification.getKey()))
                .build(CacheLoader.from(this::load));
    }

    public static OrientDbReferencedObjectProvider create(OrientDbSessionProvider querySessionProvider, Duration cacheExpirationTime) {
        return new OrientDbReferencedObjectProvider(querySessionProvider, cacheExpirationTime);
    }

    private void addListener(ORID orid, String className) {
        Listener listener = listenerByClassMap.computeIfAbsent(className, Listener::new).acquire();
        listenerByORID.put(orid, listener);
    }

    private void removeListener(ORID orid) {
        Optional.ofNullable(listenerByORID.remove(orid)).ifPresent(Listener::release);
    }

    private Optional<PropertyResolver> load(ORID id) {
        return Optional
                .ofNullable(querySessionProvider.<OElement>getWithSession(s -> s.load(id)))
                .map(e -> {
                    e.getSchemaType().map(OClass::getName).ifPresent(n -> addListener(id, n));
                    return OElementPropertyResolver.create(this, e).cache();
                });
    }

    public PropertyResolver retrieve(ORID id) {
        try {
            return propertyResolverCache.get(id)
                    .orElseGet(() -> {
                        log.warn("Could not find reference to {}", id);
                        return null;
                    });
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
