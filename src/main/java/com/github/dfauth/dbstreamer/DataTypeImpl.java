package com.github.dfauth.dbstreamer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class DataTypeImpl<T> implements DataType<T> {

    private static final Logger logger = LoggerFactory.getLogger(DataTypeImpl.class);
    private int sqlType;

    protected DataTypeImpl(int sqlType) {
        this.sqlType = sqlType;
    }

    @Override
    public int sqlType() {
        return sqlType;
    }
}
