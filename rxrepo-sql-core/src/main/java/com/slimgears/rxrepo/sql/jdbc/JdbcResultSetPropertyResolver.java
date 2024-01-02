package com.slimgears.rxrepo.sql.jdbc;

import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import com.slimgears.rxrepo.sql.FlattenedPropertyResolver;
import com.slimgears.rxrepo.sql.SqlTypeMapper;
import com.slimgears.rxrepo.util.PropertyResolver;
import com.slimgears.util.stream.Lazy;
import com.slimgears.util.stream.Safe;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

@SuppressWarnings("UnstableApiUsage")
public class JdbcResultSetPropertyResolver implements PropertyResolver {
    private final ResultSet resultSet;
    private final SqlTypeMapper typeMapper;
    private final Lazy<Map<String, Integer>> columnNames;

    public static PropertyResolver create(ResultSet resultSet, SqlTypeMapper typeMapper) {
        return FlattenedPropertyResolver.of(new JdbcResultSetPropertyResolver(resultSet, typeMapper));
    }

    private JdbcResultSetPropertyResolver(ResultSet resultSet, SqlTypeMapper typeMapper) {
        this.resultSet = resultSet;
        this.typeMapper = typeMapper;
        this.columnNames = Lazy.fromCallable(this::getColumns);
    }

    private Map<String, Integer> getColumns() throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        return IntStream.range(0, metaData.getColumnCount())
                .boxed()
                .collect(ImmutableMap.toImmutableMap(i -> {
                    try {
                        return metaData.getColumnName(i + 1);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }, i -> i));
    }

    @Override
    public Iterable<String> propertyNames() {
        return columnNames.get().keySet();
    }

    @Override
    public Object getProperty(String name, Class<?> type) {
        return Optional.ofNullable(columnNames.get().get(name))
                .map(Safe.ofFunction(index -> resultSet.getObject(index + 1)))
                .map(val -> typeMapper.fromSqlValue(TypeToken.of(type), val))
                .orElse(null);
    }
}
