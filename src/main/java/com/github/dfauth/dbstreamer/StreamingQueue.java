package com.github.dfauth.dbstreamer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Consumer;
import java.util.stream.Stream;

public class StreamingQueue<R> {

    private static final Logger logger = LoggerFactory.getLogger(StreamingQueue.class);

    private final Consumer<Sink<R>> consumer;

    public StreamingQueue(Consumer<Sink<R>> consumer) {
        this.consumer = consumer;
    }

    public Stream<R> getStream() {
        BlockingQueue<R> queue = new LinkedBlockingDeque<>(5);
        Executors.newSingleThreadExecutor().submit(() -> consumer.accept(r -> queue.offer(r)));
        return Stream.generate(() -> {
            try {
                return queue.take();
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }
        });
    }

}
