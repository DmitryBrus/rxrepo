package com.slimgears.rxrepo.orientdb;

import com.orientechnologies.orient.core.record.OElement;
import com.slimgears.rxrepo.util.PropertyResolver;

class OElementPropertyResolver extends AbstractOrientPropertyResolver {
    private final OElement oElement;

    private OElementPropertyResolver(OrientDbReferencedObjectProvider refObjectProvider, OElement oElement) {
        super(refObjectProvider);
        this.oElement = oElement;
    }

    @Override
    public Iterable<String> propertyNames() {
        return oElement.getPropertyNames();
    }

    @Override
    protected Object getPropertyInternal(String name, Class<?> type) {
        return oElement.getProperty(name);
    }

    static PropertyResolver create(OrientDbReferencedObjectProvider refObjectProvider, OElement oElement) {
        return new OElementPropertyResolver(refObjectProvider, oElement).cache();
    }
}
