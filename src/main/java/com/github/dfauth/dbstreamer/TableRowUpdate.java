package com.github.dfauth.dbstreamer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class TableRowUpdate {

    private static final Logger logger = LoggerFactory.getLogger(TableRowUpdate.class);

    private final String name;
    private final List<ColumnUpdate> updates;

    public TableRowUpdate(String name, List<ColumnUpdate> updates) {
        this.name = name;
        this.updates = updates;
    }

    @Override
    public String toString() {
        return "TableRowUpdate("+ name +", "+ updates +")";
    }

    public void addBatch(PreparedStatement pstmt) {
        try {
            int i=1;
            for(ColumnUpdate u: updates) {
                u.update(pstmt, i++);
            }
            pstmt.addBatch();
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    public List<ColumnUpdate> getColumnUpdates() {
        return updates;
    }

    public String getTable() {
        return name;
    }
}
