package com.slimgears.rxrepo.sql;

import com.google.common.reflect.TypeToken;

@SuppressWarnings("UnstableApiUsage")
public interface SqlTypeMapper {
    String toSqlType(TypeToken<?> typeToken);
    boolean isSupported(TypeToken<?> typeToken);
    <T> Object toSqlValue(T value);
    <T> T fromSqlValue(TypeToken<T> type, Object sqlValue);
}
