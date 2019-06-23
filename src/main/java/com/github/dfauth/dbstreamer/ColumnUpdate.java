package com.github.dfauth.dbstreamer;

import java.sql.PreparedStatement;

public class ColumnUpdate<T> {

    private final ColumnDefinition columnDefinition;
    private final T result;

    public ColumnUpdate(ColumnDefinition columnDefinition, T result) {
        this.columnDefinition = columnDefinition;
        this.result = result;
    }

    @Override
    public String toString() {
        return "ColumnUpdate("+ columnDefinition +", "+result+")";
    }

    public void update(PreparedStatement pstmt, int i) {
        columnDefinition.update(pstmt, i, result);
    }
}
