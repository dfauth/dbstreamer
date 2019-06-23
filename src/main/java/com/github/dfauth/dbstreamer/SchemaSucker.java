package com.github.dfauth.dbstreamer;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.*;
import java.util.concurrent.*;

public class SchemaSucker {

    private static final Logger logger = LoggerFactory.getLogger(SchemaSucker.class);
    private final DataSource source;
    private final DataSource target;
    private final List<String> excludedTables;
    private BlockingQueue<TableDefinition> queue = new LinkedBlockingQueue<>();
    private int nThreads = 6;
    private boolean shouldContinue = true;
    private TargetDatabase targetdB;
    private int batchSize = 1000;

    public SchemaSucker(DataSource source, DataSource target) {
        this(source, target, Collections.emptyList());
    }

    public SchemaSucker(DataSource source, DataSource target, List<String> excludedTables) {
        this.source = source;
        this.target = target;
        this.excludedTables = excludedTables;
    }

    public void suck() {
        targetdB = new TargetDatabase(this.target);
        targetdB.tables().stream().filter(t -> !excludedTableList().contains(t)).forEach(t -> {
            SortedSet<ColumnDefinition> columns = targetdB.columnDefs(t);
            TableDefinition td = new TableDefinition(t, columns);
            logger.info("compiled table definition "+td);
            enqueue(td);
        });
        if(!queue.isEmpty()) {
            logger.info("queue depth "+queue.size());
            try {
                targetdB.disableReferentialIntegrityChecks();
                processAsync();
                while(!queue.isEmpty()) {
                    sleep();
                }
                logger.info("queue depth "+queue.size()+" stopping... ");
                stop();
                sleep();
            } finally {
                targetdB.enableReferentialIntegrityChecks();
            }
        }
    }

    private void sleep() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            logger.info(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private void stop() {
        shouldContinue = false;
    }

    private void processAsync() {
        ExecutorService executors = Executors.newFixedThreadPool(nThreads);

        for(int i=0; i< nThreads; i++) {
            executors.submit(() -> {
                try {
                    while(shouldContinue) {
                        processTableDefinition(queue.take());
                    }
                } catch (InterruptedException e) {
                    logger.info(e.getMessage(), e);
                    throw new RuntimeException(e);
                }
            });
        }
    }

    private void processTableDefinition(TableDefinition tableDefinition) {
        SourceDatabase sourcedB = new SourceDatabase(this.source);
        Publisher<TableRowUpdate> publisher = sourcedB.asPublisherFor(tableDefinition);
        Subscriber<TableRowUpdate> subscriber = targetdB.asSubscriberFor(tableDefinition, batchSize);
        publisher.subscribe(subscriber);
    }

    private void enqueue(TableDefinition td) {
        queue.offer(td);
    }

    private List<String> excludedTableList() {
        return excludedTables;
    }
}
