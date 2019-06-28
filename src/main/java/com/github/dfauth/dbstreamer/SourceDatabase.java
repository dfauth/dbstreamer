package com.github.dfauth.dbstreamer;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class SourceDatabase extends AbstractDatabase {

    private static final Logger logger = LoggerFactory.getLogger(SourceDatabase.class);

    public SourceDatabase(DataSource dataSource) {
        super(dataSource);
    }

    public Publisher<TableRowUpdate> asPublisherFor(TableDefinition tableDef) {
        return new FluxQueueWrapper<>(createPublisherFor(tableDef)).asFlux();
    }

    private void queryStarForTable(TableDefinition tableDef, Subscriber<? super TableRowUpdate> subscriber) {
        Connection connection = null;
        try {
            connection = this.dataSource.getConnection();
            PreparedStatement pstmt = connection.prepareStatement(getColumnQuery(tableDef.getName()));
            ResultSet resultSet = pstmt.executeQuery();
            while (resultSet.next()) {
                List<ColumnUpdate> updates = tableDef.getColumnDefs().stream().map(cd -> cd.read(resultSet)).collect(Collectors.toList());
                subscriber.onNext(new TableRowUpdate(tableDef, updates));
            }
            subscriber.onComplete();
        } catch (SQLException e) {
            subscriber.onError(e);
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        } catch (RuntimeException e) {
            subscriber.onError(e);
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

    private String getColumnQuery(String tableName) {
        return String.format("select * from %s;", tableName);
    }

    private Publisher<TableRowUpdate> createPublisherFor(TableDefinition tableDef) {
        return subscriber -> subscriber.onSubscribe(new Subscription() {
            @Override
            public void request(long n) {
//                logger.info("sourcedB subsciption request("+n+")");
                Executors.newSingleThreadExecutor().submit(( )-> {
                    queryStarForTable(tableDef, subscriber);
                });
            }

            @Override
            public void cancel() {
                logger.warn("sourcedB subsciption cancel");
            }
        });
    }

    @Override
    public String toString() {
        return "sourcedB";
    }
}
