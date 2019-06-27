package com.github.dfauth.dbstreamer;

import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

public class DbStreamer {

    private static final Logger logger = LoggerFactory.getLogger(DbStreamer.class);
    private final DataSource source;
    private final DataSource target;
    private final List<String> excludedTables;
    private BlockingQueue<TableDefinition> queue = new LinkedBlockingQueue<>();
    private int nThreads = 10;
    private boolean shouldContinue = true;
    private TargetDatabase targetdB;
    private int batchSize = 10000;
    private CountDownLatch latch;
    private BiFunction<String, ColumnDefinition, ColumnDefinition> bf = (t, cd) -> cd;
    private Function<String, UnaryOperator<ColumnUpdate>> ff = t -> UnaryOperator.identity();

    public DbStreamer(DataSource source, DataSource target) {
        this(source, target, Collections.emptyList());
    }

    public DbStreamer(DataSource source, DataSource target, List<String> excludedTables) {
        this.source = source;
        this.target = target;
        this.excludedTables = excludedTables;
    }

    public void stream() {
        targetdB = new TargetDatabase(this.target);
        targetdB.tables().stream().filter(t -> !excludedTableList().contains(t)).forEach(t -> {
            SortedSet<ColumnDefinition> columns = targetdB.columnDefs(t).stream().map(cd -> bf.apply(t, cd)).collect(Collectors.toCollection((() -> new TreeSet(ColumnDefinition.comparator))));
            TableDefinition td = new TableDefinition(t, columns);
            logger.info("compiled table definition "+td);
            enqueue(td);
        });
        if(!queue.isEmpty()) {
            logger.info("queue depth "+queue.size());
            try {
                targetdB.disableReferentialIntegrityChecks();
                sleep();
                processAsync();
                while(!queue.isEmpty()) {
                    sleep();
                    logger.info("awoke, queue depth "+queue.size());
                }
                logger.info("queue depth "+queue.size()+" stopping... ");
                stop();
                waitForWorkerCompletion();
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
                throw new RuntimeException(e);
            } finally {
                targetdB.enableReferentialIntegrityChecks();
            }
        }
    }

    private void sleep() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private void stop() {
        shouldContinue = false;
    }

    private void processAsync() {
        ExecutorService executors = Executors.newFixedThreadPool(nThreads);

        latch = new CountDownLatch(nThreads);
        for(int i=0; i< nThreads; i++) {
            executors.submit(() -> {
                try {
                    while(shouldContinue) {
                        TableDefinition tableDef = queue.poll(1, TimeUnit.SECONDS);
                        if(tableDef != null) {
                            processTableDefinition(tableDef);
                        }
                    }
                    latch.countDown();
                } catch (InterruptedException e) {
                    logger.error(e.getMessage(), e);
                    throw new RuntimeException(e);
                }
            });
        }
    }

    private void waitForWorkerCompletion() throws InterruptedException {
        if(latch != null) {
            latch.await();
        }
    }

    private void processTableDefinition(TableDefinition tableDefinition) {
        try {
            SourceDatabase sourcedB = new SourceDatabase(this.source);
            Publisher<TableRowUpdate> publisher = sourcedB.asPublisherFor(tableDefinition);
            Subscriber<TableRowUpdate> subscriber = targetdB.asSubscriberFor(tableDefinition, batchSize);
            Processor<TableRowUpdate, TableRowUpdate> processor = getProcessor(publisher, subscriber);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private Processor<TableRowUpdate, TableRowUpdate> getProcessor(Publisher<TableRowUpdate> publisher, Subscriber<TableRowUpdate> subscriber) {
        Processor<TableRowUpdate, TableRowUpdate> processor = new Processor<TableRowUpdate, TableRowUpdate>() {
            private Subscriber<? super TableRowUpdate> subscriber;

            @Override
            public void subscribe(Subscriber<? super TableRowUpdate> s) {
                this.subscriber = s;
            }

            @Override
            public void onSubscribe(Subscription s) {
                if (this.subscriber != null) {
                    this.subscriber.onSubscribe(s);
                }
            }

            @Override
            public void onNext(TableRowUpdate tru) {
                List<ColumnUpdate> updates = tru.getColumnUpdates().stream().map(c -> ff.apply(tru.getTable()).apply(c)).collect(Collectors.toList());
                this.subscriber.onNext(new TableRowUpdate(tru.getTable(), updates));
            }

            @Override
            public void onError(Throwable t) {
                this.subscriber.onError(t);
            }

            @Override
            public void onComplete() {
                this.subscriber.onComplete();
            }
        };
        processor.subscribe(subscriber);
        publisher.subscribe(processor);
        return processor;
    }

    private void enqueue(TableDefinition td) {
        queue.offer(td);
    }

    private List<String> excludedTableList() {
        return excludedTables;
    }

    public DbStreamer withColumnDefinition(BiFunction<String, ColumnDefinition, ColumnDefinition> f) {
        this.bf = f;
        return this;
    }

    public DbStreamer withColumnUpdate(BiFunction<String, ColumnUpdate, ColumnUpdate> f) {
        this.ff = curry(f);
        return this;
    }

    public DbStreamer withColumnUpdate(Function<String, UnaryOperator<ColumnUpdate>> f) {
        this.ff = f;
        return this;
    }

    public DbStreamer withColumnUpdate(Predicate<String> filter, UnaryOperator<ColumnUpdate> f) {
        this.ff = FilteringFunction.of(filter, f);
        return this;
    }

    public DbStreamer withColumnDefinition(Function<String, UnaryOperator<ColumnDefinition>> f) {
        this.bf = toBiFunctionFromUnaryOperator(f);
        return this;
    }

    private <T,U> BiFunction<T, U, U> toBiFunctionFromUnaryOperator(Function<T, UnaryOperator<U>> f) {
        return toBiFunction(t -> f.apply(t));
    }

    private <T,U,V> BiFunction<T, U, V> toBiFunction(Function<T, Function<U, V>> f) {
        return (T t, U u) -> f.apply(t).apply(u);
    }

    public <T,U> Function<T, UnaryOperator<U>> curry(BiFunction<T,U,U> f) {
        return (T t) -> (U u) -> f.apply(t,u);
    }
}
