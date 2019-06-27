package com.github.dfauth.dbstreamer;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.IntStream;

public class TargetDatabase {

    private static final Logger logger = LoggerFactory.getLogger(TargetDatabase.class);

    private final DataSource dataSource;

    public TargetDatabase(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public List<String> tables() {
        Connection connection = null;
        try {
            List<String> tmp = new ArrayList<>();
            connection = this.dataSource.getConnection();
            PreparedStatement pstmt = connection.prepareStatement(getTableQuery());
            ResultSet resultSet = pstmt.executeQuery();
            while (resultSet.next()) {
                String tableName = resultSet.getString(1);
                tmp.add(tableName);
            }
            return tmp;
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        } finally {
            if(connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
    }

    private ColumnDefinition toColumnDefinition(String table, int ordinalPosition, String columnName, String sqlDataType) {
        DataType dataType = DataType.Factory.findBySqlType(sqlDataType).create();
        return new ColumnDefinition(table, ordinalPosition, columnName, dataType);
    }

    private String getTableQuery() {
        return "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA='PUBLIC'";
    }

    private String getColumnQuery(String tableName) {
        return String.format("SELECT COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='%s'", tableName);
    }

    public SortedSet<ColumnDefinition> columnDefs(String table) {
        Connection connection = null;
        try {
            SortedSet<ColumnDefinition> tmp = new TreeSet<>(ColumnDefinition.comparator);
            connection = this.dataSource.getConnection();
            PreparedStatement pstmt = connection.prepareStatement(getColumnQuery(table));
            ResultSet resultSet = pstmt.executeQuery();
            while (resultSet.next()) {
                String columnName = resultSet.getString(1);
                int ordinalPosition = resultSet.getInt(2);
                String dataType = resultSet.getString(3);
                ColumnDefinition columnDef = toColumnDefinition(table, ordinalPosition, columnName, dataType);
                tmp.add(columnDef);
            }
            return tmp;
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        } finally {
            if(connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
    }

    public Subscriber<TableRowUpdate> asSubscriberFor(TableDefinition tableDefinition) {
        return asSubscriberFor(tableDefinition, 1000);
    }

    public Subscriber<TableRowUpdate> asSubscriberFor(TableDefinition tableDefinition, int batchSize) {

        return new Subscriber<TableRowUpdate>() {
            private PreparedStatement pstmt;
            Connection connection = null;
            int cnt = 0;
            @Override
            public void onSubscribe(Subscription s) {
                try {
                    s.request(Long.MAX_VALUE);
                    connection = dataSource.getConnection();
                    pstmt = connection.prepareStatement(insertStatement(tableDefinition));
                } catch (SQLException e) {
                    logger.error(e.getMessage(), e);
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void onNext(TableRowUpdate tableRowUpdate) {
                try {
                    tableRowUpdate.addBatch(pstmt);
                    cnt++;
                    if(cnt%batchSize == 0) {
                        int[] result = pstmt.executeBatch();
                        logger.info("onNext: cnt: "+cnt+" pstmt.executeBatch() result: "+ IntStream.of(result).mapToObj(i -> i == 1).reduce((b1, b2) -> b1 & b2).orElse(false));
                    }
                } catch (SQLException e) {
                    logger.error(e.getMessage(), e);
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void onError(Throwable t) {
                logger.error("onError("+t+")");
                close();
            }

            @Override
            public void onComplete() {
                try {
                    if(cnt%batchSize != 0) {
                        int[] result = pstmt.executeBatch();
                        logger.info("onComplete:  cnt: "+cnt+" pstmt.executeBatch() result: "+ IntStream.of(result).mapToObj(i -> i == 1).reduce((b1, b2) -> b1 & b2).orElse(false));
                    }
                    close();
                } catch (SQLException e) {
                    logger.error(e.getMessage(), e);
                    throw new RuntimeException(e);
                }
            }

            private void close() {
                if(connection != null) {
                    try {
                        connection.close();
                    } catch (SQLException e) {
                        logger.error(e.getMessage(), e);
                    }
                }
            }
        };
    }

    private String insertStatement(TableDefinition tableDef) {
        Supplier<IllegalArgumentException> supplier = () -> new IllegalArgumentException("table definition "+tableDef+" contains no columns");
        Optional<String> columns = tableDef.getColumnDefs().stream().map(cd -> cd.getName()).reduce((s, s2) -> s + ","+s2);
        Optional<String> questionMarks = tableDef.getColumnDefs().stream().map(cd -> cd.getName()).map(v -> "?").reduce((s, s2) -> s+","+s2);
        return columns.flatMap(s -> questionMarks.map(r -> String.format("insert into %s (%s) values (%s);", tableDef.getName(), s, r))).orElseThrow(supplier);
    }

    public void enableReferentialIntegrityChecks() {
        executePreparedStatement("SET DATABASE REFERENTIAL INTEGRITY TRUE;");
    }

    public void disableReferentialIntegrityChecks() {
        executePreparedStatement("SET DATABASE REFERENTIAL INTEGRITY FALSE;");
    }

    public void executePreparedStatement(String sql) {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            PreparedStatement pstmt = connection.prepareStatement(sql);
            int result = pstmt.executeUpdate();
            logger.info("result: " + result + " for sql: " + sql);
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        } finally {
            if(connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    logger.error(e.getMessage(), e);
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
