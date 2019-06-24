package com.github.dfauth.dbstreamer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.SortedSet;

public class ColumnDefinition<T> {

    private static final Logger logger = LoggerFactory.getLogger(ColumnDefinition.class);
    public static Comparator<ColumnDefinition> comparator = Comparator.comparingInt(ColumnDefinition::getOrdinal);

    private final int ord;
    private final String name;
    private final DataType<T> dataType;

    public ColumnDefinition(int ordinalPosition, String columnName, DataType<T> dataType) {
        this.ord = ordinalPosition;
        this.name = columnName;
        this.dataType = dataType;
    }

    @Override
    public String toString() {
        return "ColumnDefinition["+ord+", "+name+"]";
    }

    public int getOrdinal() {
        return ord;
    }

    public ColumnUpdate read(ResultSet rs) {
        T val = dataType.getReadFunction(ord).apply(rs);
        return new ColumnUpdate(this, val);
    }

    public String getName() {
        return name;
    }

    public void update(PreparedStatement pstmt, int i, T result) {
        try {
            if(result == null) {
                pstmt.setNull(i, dataType.sqlType());
            } else {
                dataType.getWriteConsumer(pstmt, i).accept(result);
            }
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    public DataType<T> getDataType() {
        return dataType;
    }
}
