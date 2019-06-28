package com.github.dfauth.dbstreamer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class TableRowUpdate {

    private static final Logger logger = LoggerFactory.getLogger(TableRowUpdate.class);

    private final List<ColumnUpdate> updates;
    private final TableDefinition tableDef;

    public TableRowUpdate(TableDefinition tableDef, List<ColumnUpdate> updates) {
        this.tableDef = tableDef;
        this.updates = updates;
    }

    @Override
    public String toString() {
        return "TableRowUpdate("+ tableDef.getName() +", "+ updates +")";
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
        return tableDef.getName();
    }

    public TableDefinition getTableDefinition() {
        return tableDef;
    }
}
