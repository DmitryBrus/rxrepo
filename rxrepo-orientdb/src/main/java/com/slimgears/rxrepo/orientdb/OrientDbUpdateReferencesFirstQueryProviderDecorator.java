package com.slimgears.rxrepo.orientdb;

import com.slimgears.rxrepo.query.decorator.UpdateReferencesFirstQueryProviderDecorator;
import com.slimgears.rxrepo.query.provider.QueryProvider;
import com.slimgears.util.autovalue.annotations.MetaClassWithKey;
import io.reactivex.Completable;
import io.reactivex.Maybe;
import io.reactivex.functions.Function;

import java.util.function.Supplier;

public class OrientDbUpdateReferencesFirstQueryProviderDecorator extends UpdateReferencesFirstQueryProviderDecorator {
    private OrientDbUpdateReferencesFirstQueryProviderDecorator(QueryProvider underlyingProvider) {
        super(underlyingProvider);
    }

    public static Decorator create() {
        return OrientDbUpdateReferencesFirstQueryProviderDecorator::new;
    }

    @Override
    public <K, S> Completable insert(MetaClassWithKey<K, S> metaClass, Iterable<S> entities, boolean recursive) {
        return super.insert(metaClass, entities, false);
    }
}
