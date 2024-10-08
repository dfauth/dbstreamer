package com.github.dfauth.dbstreamer;

import org.reactivestreams.Processor;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;
import java.util.stream.Collectors;

public class DbStreamer {

    private static final Logger logger = LoggerFactory.getLogger(DbStreamer.class);
    private final DataSource source;
    private final DataSource target;
    private Predicate<String> excludedTables = t -> false; //t -> !excludedTableList().contains(t);
    private Predicate<String> includedTables = t -> true;
    private BlockingQueue<TableDefinition> inQueue = new LinkedBlockingQueue<>();
    private BlockingQueue<Boolean> outQueue = new LinkedBlockingQueue<>();
    private boolean shouldContinue = true;
    private TargetDatabase targetdB;
    private int batchSize = 10000;
    private BiFunction<String, ColumnDefinition, ColumnDefinition> bf = (t, cd) -> cd;
    private Function<String, UnaryOperator<ColumnUpdate>> ff = t -> UnaryOperator.identity();
    private SourceDatabase sourcedB;

    public DbStreamer(DataSource source, DataSource target) {
        this.source = source;
        this.target = target;
    }

    public List<TableDefinition> sniff() {
        List<TableDefinition> tmp = new ArrayList<>();
        sniff(td -> tmp.add(td));
        return tmp;
    }

    public void sniff(Consumer<TableDefinition> consumer) {
        sourcedB = new SourceDatabase(this.source);
        targetdB = new TargetDatabase(this.target);
        targetdB.tables().stream()
                .filter(excludedTables.negate())
                .filter(includedTables)
                .forEach(t -> {
            SortedSet<ColumnDefinition> columns = targetdB.columnDefs(t).stream().map(cd -> bf.apply(t, cd)).collect(Collectors.toCollection((() -> new TreeSet<>(ColumnDefinition.comparator))));
            TableDefinition td = new TableDefinition(t, columns);
            logger.info("compiled table definition "+td);
            consumer.accept(td);
        });
    }

    public void stream() {
        stream(td -> {});
    }

    public void stream(Consumer<TableDefinition> consumer) {
        sniff(td -> {
            try {
                enqueue(td);
                consumer.accept(td);
            } catch(RuntimeException e) {
                logger.error(e.getMessage(), e);
            }
        });
        if(!inQueue.isEmpty()) {
            logger.info("queue depth "+ inQueue.size());
            try {
                targetdB.disableReferentialIntegrityChecks();
                sleep();
                int qSize = inQueue.size();
                processAsync();
                while(!inQueue.isEmpty()) {
                    sleep();
                    logger.info("awoke, queue depth "+ inQueue.size());
                }
                logger.info("queue depth "+ inQueue.size()+" stopping... ");
                stop();
                waitForWorkerCompletion(qSize);
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
        ExecutorService executors = ForkJoinPool.commonPool();

        while(inQueue.size() > 0) {
            executors.submit(() -> {
                try {
                    while(shouldContinue) {
                        TableDefinition tableDef = inQueue.poll(1, TimeUnit.SECONDS);
                        if(tableDef != null) {
                            processTableDefinition(tableDef).thenApply(outQueue::offer);
                        }
                    }
                } catch (InterruptedException e) {
                    logger.error(e.getMessage(), e);
                    throw new RuntimeException(e);
                }
            });
        }
    }

    private void waitForWorkerCompletion(int qSize) throws InterruptedException {
        while(outQueue.size() != qSize) {
            synchronized (inQueue) {
                inQueue.wait(100000);
            }
        }
    }

    private CompletableFuture<Boolean> processTableDefinition(TableDefinition tableDefinition) {
        try {
            CompletableFuture<Boolean> fut = new CompletableFuture<>();
            Publisher<TableRowUpdate> publisher = sourcedB.asPublisherFor(tableDefinition);
            Subscriber<TableRowUpdate> subscriber = targetdB.asSubscriberFor(tableDefinition, batchSize, fut::complete, fut::completeExceptionally);
            Processor<TableRowUpdate, TableRowUpdate> processor = getProcessor(publisher, subscriber);
            return fut;
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
                UnaryOperator<ColumnUpdate> f = ff.apply(tru.getTable());
                List<ColumnUpdate> updates = tru.getColumnUpdates().stream().map(c -> f.apply(c)).collect(Collectors.toList());
                this.subscriber.onNext(new TableRowUpdate(tru.getTableDefinition(), updates));
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
        inQueue.offer(td);
    }

    public DbStreamer excludeTables(Predicate<String> excluded) {
        this.excludedTables = excluded;
        return this;
    }

    public DbStreamer includeTables(Predicate<String> included) {
        this.includedTables = included;
        return this;
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

    public TargetDatabase getTargetdB() {
        return targetdB;
    }

    public SourceDatabase getSourcedB() {
        return sourcedB;
    }
}
