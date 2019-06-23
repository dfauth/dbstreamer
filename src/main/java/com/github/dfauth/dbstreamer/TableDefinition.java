package com.github.dfauth.dbstreamer;

import java.util.SortedSet;

public class TableDefinition {

    private final String name;
    private final SortedSet<ColumnDefinition> columns;

    public TableDefinition(String name, SortedSet<ColumnDefinition> columns) {
        this.name = name;
        this.columns = columns;
    }

    public String getName() {
        return name;
    }

    public SortedSet<ColumnDefinition> getColumnDefs() {
        return columns;
    }

    @Override
    public String toString() {
        return "TableDefinition("+name+")";
    }
}
