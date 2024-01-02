package com.slimgears.rxrepo.mem;

import com.slimgears.rxrepo.encoding.MetaObjectResolver;
import com.slimgears.rxrepo.query.provider.AbstractEntityQueryProviderAdapter;
import com.slimgears.rxrepo.query.provider.EntityQueryProvider;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import com.slimgears.util.stream.Safe;
import io.reactivex.Completable;
import io.reactivex.Maybe;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class MemoryQueryProvider extends AbstractEntityQueryProviderAdapter implements MetaObjectResolver {
    private final List<AutoCloseable> closeableList = Collections.synchronizedList(new ArrayList<>());
    private final AtomicLong sequenceNumber = new AtomicLong();

    public static MemoryQueryProvider create() {
        return new MemoryQueryProvider();
    }

    @Override
    protected <K, S> EntityQueryProvider<K, S> createProvider(MetaClassWithKey<K, S> metaClass) {
        MemoryEntityQueryProvider<K, S> provider = MemoryEntityQueryProvider.create(metaClass, this, sequenceNumber);
        closeableList.add(provider);
        return provider;
    }

    @Override
    protected Completable dropAllProviders() {
        return Completable.complete();
    }

    @Override
    public <K, S> Maybe<S> resolve(MetaClassWithKey<K, S> metaClass, K key) {
        return ((MemoryEntityQueryProvider<K, S>)entities(metaClass)).find(key);
    }

    @Override
    public void close() {
        closeableList.stream()
                .map(Safe::ofClosable)
                .forEach(Safe.Closeable::close);
    }
}
