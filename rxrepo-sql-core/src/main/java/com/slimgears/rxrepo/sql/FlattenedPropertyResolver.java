package com.slimgears.rxrepo.sql;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.reflect.TypeToken;
import com.slimgears.rxrepo.util.PropertyResolver;
import com.slimgears.util.stream.Lazy;

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.*;

@SuppressWarnings("UnstableApiUsage")
public class FlattenedPropertyResolver implements PropertyResolver {
    private final PropertyResolver underlyingResolver;
    private final String delimiter;
    private final String prefix;
    private final int index;
    private final Lazy<Iterable<String>> propertyNames;
    private final Map<String, Lazy<PropertyResolver>> resolvers = new HashMap<>();
    private final BiMap<String, String> propertyToCanonicNameMap = HashBiMap.create();
    private final BiMap<String, String> propertyFromCanonicNameMap = propertyToCanonicNameMap.inverse();

    public static PropertyResolver of(PropertyResolver underlyingResolver) {
        return of(underlyingResolver, ".");
    }

    public static PropertyResolver of(PropertyResolver underlyingResolver, String delimiter) {
        return new FlattenedPropertyResolver(underlyingResolver, Pattern.quote(delimiter));
    }

    private FlattenedPropertyResolver(PropertyResolver underlyingResolver, String delimiter) {
        this(underlyingResolver, delimiter, "");
    }

    private FlattenedPropertyResolver(PropertyResolver underlyingResolver, String delimiter, String prefix) {
        this.delimiter = delimiter;
        this.underlyingResolver = underlyingResolver;
        this.prefix = prefix;
        this.index = (int)Arrays.stream(split(prefix)).filter(p -> !p.isEmpty()).count();
        this.propertyNames = Lazy.of(this::retrievePropertyNames);
    }

    @Override
    public Iterable<String> propertyNames() {
        return propertyNames.get();
    }

    @Override
    public Object getProperty(String name, Class<?> type) {
        return getPropertyInternal(name, type);
    }

    protected Object getPropertyInternal(String name, Class<?> type) {
        propertyNames.get();
        return Optional
                .ofNullable(resolvers.get(fromCanonic(name)))
                .map(Lazy::get)
                .map(Object.class::cast)
                .orElseGet(() -> {
                    Object obj = underlyingResolver.getProperty(prefix + fromCanonic(name), type);
                    if (obj instanceof PropertyResolver) {
                        PropertyResolver resolver = (PropertyResolver)obj;
                        resolvers.put(prefix + fromCanonic(name), Lazy.of(() -> resolver));
                        obj = resolver;
                    }
                    return obj;
                });
    }

    private Collection<String> getResultPropertyNames() {
        return ImmutableList.copyOf(underlyingResolver.propertyNames());
    }

    private Iterable<String> retrievePropertyNames() {
        Collection<String> underlyingPropertyNames = getResultPropertyNames();
        Map<String, List<String>> map = underlyingPropertyNames
                .stream()
                .filter(n -> n.startsWith(prefix))
//                .filter(n -> underlyingResolver.getProperty(n, TypeToken.of(PropertyResolver.class)) != null)
                .peek(this::toCanonic)
                .collect(groupingBy(
                        name -> split(name)[index],
                        mapping(name -> name.substring(prefix.length()), toList())));

        map.entrySet()
                .stream()
                .filter(e -> !e.getValue().isEmpty() && e.getValue().get(0).length() > e.getKey().length())
                .forEach(e -> resolvers.put(e.getKey(), Lazy.of(() -> new FlattenedPropertyResolver(underlyingResolver, delimiter, prefix + e.getKey() + "."))));

        return map.keySet().stream().map(this::toCanonic).collect(Collectors.toSet());
    }

    private String toCanonic(String name) {
        return propertyToCanonicNameMap.computeIfAbsent(name, n -> n.replace("`", ""));
    }

    private String fromCanonic(String name) {
        return propertyFromCanonicNameMap.getOrDefault(name, name);
    }

    private String[] split(String name) {
        String[] parts = name.split(delimiter);
        return parts.length > 0 ? parts : new String[] {name};
    }
}
