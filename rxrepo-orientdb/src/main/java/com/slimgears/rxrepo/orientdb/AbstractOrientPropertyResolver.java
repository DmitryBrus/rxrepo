package com.slimgears.rxrepo.orientdb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OTrackedList;
import com.orientechnologies.orient.core.db.record.OTrackedMap;
import com.orientechnologies.orient.core.db.record.OTrackedSet;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.slimgears.rxrepo.util.PropertyResolver;
import com.slimgears.util.stream.Streams;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public abstract class AbstractOrientPropertyResolver implements PropertyResolver {
    final OrientDbReferencedObjectProvider refObjectProvider;

    AbstractOrientPropertyResolver(OrientDbReferencedObjectProvider refObjectProvider) {
        this.refObjectProvider = refObjectProvider;
    }

    @Override
    public Object getProperty(String name, Class<?> type) {
        Object obj = getPropertyInternal(name, type);
        return toValue(obj, type);
    }

    private Object toValue(Object obj, Class<?> expectedType) {
        return toValue(refObjectProvider, obj, expectedType);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Object toValue(OrientDbReferencedObjectProvider refObjectProvider, Object obj, Class<?> expectedType) {
        if (obj instanceof OElement) {
            return OElementPropertyResolver.create(refObjectProvider, (OElement)obj);
        } else if (expectedType.isEnum() && obj != null) {
            return Enum.valueOf((Class)expectedType, obj.toString());
        } else if (obj instanceof ORID) {
            //return toValue(dbSessionProvider, dbSessionProvider.getWithSession(s -> s.load((ORecordId)obj)), expectedType);
            return refObjectProvider.retrieve((ORID)obj);
        } else if (obj instanceof OTrackedList) {
            return ((OTrackedList<?>)obj)
                    .stream()
                    .map(v -> toValue(refObjectProvider, v, expectedType))
                    .collect(ImmutableList.toImmutableList());
        } else if (obj instanceof OTrackedSet) {
            return ((OTrackedSet<?>)obj)
                    .stream()
                    .map(v -> toValue(refObjectProvider, v, expectedType))
                    .collect(ImmutableSet.toImmutableSet());
        } else if (obj instanceof OTrackedMap) {
            return ((OTrackedMap<?>)obj)
                    .entrySet()
                    .stream()
                    .collect(ImmutableMap.toImmutableMap(
                            Map.Entry::getKey,
                            e -> toValue(refObjectProvider, e.getValue(), expectedType)));
        } else if (obj instanceof OResult) {
            return OResultPropertyResolver.create(refObjectProvider, (OResult)obj);
        } else if (obj instanceof OResultSet) {
            return ((OResultSet)obj)
                    .stream()
                    .map(r -> toValue(refObjectProvider, r, expectedType))
                    .collect(ImmutableList.toImmutableList());
        } else if (obj instanceof Iterable) {
            Stream<?> stream = Streams.fromIterable((Iterable<?>)obj)
                    .map(o -> toValue(refObjectProvider, o, expectedType));
            if (obj instanceof List) {
                return stream.collect(ImmutableList.toImmutableList());
            } else if (obj instanceof Set) {
                return stream.collect(ImmutableSet.toImmutableSet());
            }
        } else if (obj instanceof Map) {
            return ((Map<?, ?>)obj).entrySet().stream()
                    .collect(ImmutableMap.toImmutableMap(
                            e -> toValue(refObjectProvider, e.getKey(), expectedType),
                            e -> toValue(refObjectProvider, e.getValue(), expectedType)));
        }
        return obj;
    }

    protected abstract Object getPropertyInternal(String name, Class<?> type);
}
