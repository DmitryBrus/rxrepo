package com.slimgears.rxrepo.sql.jdbc;

import com.slimgears.rxrepo.sql.AbstractSqlStatementExecutorDecorator;
import com.slimgears.rxrepo.sql.KeyEncoder;
import com.slimgears.rxrepo.sql.SqlStatementExecutor;
import com.slimgears.rxrepo.sql.SqlTypeMapper;
import com.slimgears.util.autovalue.annotations.HasMetaClass;
import com.slimgears.util.autovalue.annotations.HasMetaClassWithKey;

public class JdbcSqlStatementExecutorDecorator extends AbstractSqlStatementExecutorDecorator {
    private final SqlTypeMapper typeMapper;
    private final KeyEncoder keyEncoder;

    private JdbcSqlStatementExecutorDecorator(SqlStatementExecutor underlyingExecutor, SqlTypeMapper typeMapper, KeyEncoder keyEncoder) {
        super(underlyingExecutor);
        this.typeMapper = typeMapper;
        this.keyEncoder = keyEncoder;
    }

    public static SqlStatementExecutor.Decorator create(SqlTypeMapper typeMapper, KeyEncoder keyEncoder) {
        return src -> new JdbcSqlStatementExecutorDecorator(src, typeMapper, keyEncoder);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    @Override
    protected Object mapArgument(Object arg) {
        if (arg instanceof HasMetaClassWithKey) {
            arg = ((HasMetaClassWithKey)arg).metaClass().keyOf(arg);
        }
        if (arg instanceof HasMetaClass) {
            arg = keyEncoder.encode(arg);
        } else if (arg != null) {
            arg = typeMapper.toSqlValue(arg);
        }
        return arg;
    }
}
