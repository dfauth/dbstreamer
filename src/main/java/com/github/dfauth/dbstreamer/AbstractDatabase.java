package com.github.dfauth.dbstreamer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Function;


public abstract class AbstractDatabase {

    protected static final Logger logger = LoggerFactory.getLogger(AbstractDatabase.class);
    protected final DataSource dataSource;

    public AbstractDatabase(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public <T> T executePreparedStatementForQuery(String sql, Function<ResultSet, T> f) {
        return executePreparedStatement(sql, p -> {
            try {
                return f.apply(p.executeQuery());
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }
        });
    }

    public int executePreparedStatement(String sql) {
        return executePreparedStatement(sql, p -> {
            try {
                return p.executeUpdate();
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }
        });
    }

    public <T> T executePreparedStatement(String sql, Function<PreparedStatement, T> f) {
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            PreparedStatement pstmt = connection.prepareStatement(sql);
            T result = f.apply(pstmt);
            logger.info("result: " + result + " for sql: " + sql);
            return result;
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

    public int countRows(String table) {
        return executePreparedStatementForQuery(String.format("SELECT COUNT(*) FROM %s;", table), rs -> {
            try {
                while(rs.next()) {
                    return rs.getInt(1);
                }
            } catch (SQLException e) {
                logger.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }
            throw new IllegalStateException("No result returned"); // should never happen
        });
    }
}
