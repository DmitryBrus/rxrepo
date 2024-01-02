package com.slimgears.rxrepo.sql;

import com.google.common.reflect.TypeToken;
import com.slimgears.util.reflect.TypeTokens;
import com.slimgears.util.stream.Optionals;

import java.util.*;
import java.util.function.Function;

@SuppressWarnings("UnstableApiUsage")
public class SqlTypes implements SqlTypeMapper {
    interface TypeConverter<T> {
        String sqlType();
        Object toSqlValue(T value);
        T fromSqlValue(Object sqlValue);

        static <T, S> ProviderFactory<T, S> forConverter(Class<T> valueClass, Class<S> sqlClass, String sqlType) {
            return new ProviderFactory<>(sqlType, valueClass, sqlClass);
        }

        static <T, S> TypeConverter<T> converter(String sqlType, Function<T, S> toSqlValue, Function<S, T> fromSqlValue) {
            return new TypeConverter<T>() {
                @Override
                public String sqlType() {
                    return sqlType;
                }

                @SuppressWarnings("unchecked")
                @Override
                public Object toSqlValue(T value) {
                    return toSqlValue.apply(value);
                }

                @SuppressWarnings("unchecked")
                @Override
                public T fromSqlValue(Object sqlValue) {
                    return fromSqlValue.apply((S)sqlValue);
                }
            };
        }

        class ProviderFactory<T, S> {
            private final String sqlType;
            private final Class<T> valueClass;
            private final Class<S> sqlClass;

            public ProviderFactory(String sqlType, Class<T> valueClass, Class<S> sqlClass) {
                this.sqlType = sqlType;
                this.valueClass = valueClass;
                this.sqlClass = sqlClass;
            }

            public Provider convertWith(Function<T, S> toSqlValue, Function<S, T> fromSqlValue) {
                return new Provider() {
                    @SuppressWarnings("unchecked")
                    @Override
                    public <_T> TypeConverter<_T> provide(Class<_T> cls) {
                        return cls.equals(valueClass)
                                ? (TypeConverter<_T>)converter(sqlType, toSqlValue, fromSqlValue)
                                : null;
                    }
                };
            }
        }

        static Provider forMap(Map<Class<?>, TypeConverter<?>> map) {
            return new Provider() {
                @SuppressWarnings("unchecked")
                @Override
                public <T> TypeConverter<T> provide(Class<T> cls) {
                    return (TypeConverter<T>) Optional.ofNullable(map.get(cls)).orElse(null);
                }
            };
        }

        static Provider forEnum() {
            return new Provider() {
                @Override
                public <T> TypeConverter<T> provide(Class<T> cls) {
                    if (!cls.isEnum()) {
                        return null;
                    }

                    return new TypeConverter<T>() {
                        @Override
                        public String sqlType() {
                            return "VARCHAR";
                        }

                        @Override
                        public Object toSqlValue(T value) {
                            return value.toString();
                        }

                        @SuppressWarnings({"unchecked", "rawtypes"})
                        @Override
                        public T fromSqlValue(Object sqlValue) {
                            return (T)Enum.valueOf((Class<? extends Enum>)cls, sqlValue.toString());
                        }
                    };
                }
            };
        }

        interface Provider {
            <T> TypeConverter<T> provide(Class<T> cls);
        }
    }

    private final static Map<Class<?>, TypeConverter<?>> sqlTypes = new HashMap<>();
    private final static Collection<TypeConverter.Provider> typeConverterProviders = new ArrayList<>();

    public static final SqlTypes instance = new SqlTypes();

    static {
        registerType(TypeConverter.forEnum());
        registerType(TypeConverter.forMap(sqlTypes));
        registerType(TypeConverter.forConverter(Date.class, java.sql.Date.class, "TIMESTAMP")
                .convertWith(date -> new java.sql.Date(date.getTime()), date -> new Date(date.getTime())));

        registerType("INTEGER", Integer.class, int.class);
        registerType("BIGINT", Long.class, long.class);
        registerType("DOUBLE", Double.class, double.class);
        registerType("FLOAT", Float.class, float.class);
        registerType("SMALLINT", Short.class, short.class);
        registerType("TINYINT", Byte.class, byte.class);
        registerType("VARCHAR", String.class);
        registerType("BLOB", byte[].class);
        registerType("JSON", List.class, Set.class, Object[].class);
    }

    protected static void registerType(String sqlType, Class<?>... types) {
        Arrays.asList(types).forEach(t -> sqlTypes.put(t, TypeConverter.converter(sqlType, Function.identity(), Function.identity())));
    }

    private static void registerType(TypeConverter.Provider provider) {
        typeConverterProviders.add(provider);
    }

    @Override
    public String toSqlType(TypeToken<?> typeToken) {
        return toTypeConverter(typeToken).sqlType();
    }

    private <T> TypeConverter<T> toTypeConverter(TypeToken<T> typeToken) {
        return toTypeConverterOptional(typeToken)
                .orElseThrow(() -> new RuntimeException("Could not map type " + typeToken.toString() + " to SQL type"));
    }

    @SuppressWarnings("unchecked")
    private <T> Optional<TypeConverter<T>> toTypeConverterOptional(TypeToken<T> typeToken) {
        Class<T> cls = (Class<T>)typeToken.getRawType();
        return typeConverterProviders.stream()
                .map(provider -> provider.<T>provide(cls))
                .filter(Objects::nonNull)
                .findFirst();
    }

    @Override
    public boolean isSupported(TypeToken<?> typeToken) {
        return toTypeConverterOptional(typeToken).isPresent();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> Object toSqlValue(T value) {
        return Optional.ofNullable(value)
                .map(val -> this.toTypeConverter(TypeToken.of((Class<T>)value.getClass())).toSqlValue(value))
                .orElse(null);
    }

    @Override
    public <T> T fromSqlValue(TypeToken<T> type, Object sqlValue) {
        return Optionals.or(
                () -> Optional
                        .of(sqlValue)
                        .flatMap(Optionals.ofType(TypeTokens.asClass(type))),
                () -> toTypeConverterOptional(type)
                        .map(c -> c.fromSqlValue(sqlValue)))
                .orElse(null);
    }
}
