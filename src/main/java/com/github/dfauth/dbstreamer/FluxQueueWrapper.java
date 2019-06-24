package com.github.dfauth.dbstreamer;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SynchronousSink;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.function.Consumer;

public class FluxQueueWrapper<R> implements Subscriber<R> {

    private static final Logger logger = LoggerFactory.getLogger(FluxQueueWrapper.class);

    private final Publisher<R> publisher;
    BlockingQueue<Consumer<SynchronousSink<R>>> queue = new LinkedBlockingDeque<>(5000);
    private Subscription subscription;

    public FluxQueueWrapper(Publisher<R> publisher) {
        this.publisher = publisher;
    }

    public Flux<R> asFlux() {
        Executors.newSingleThreadExecutor().submit(run());
        return Flux.generate(s -> {
            try {
                Consumer<SynchronousSink<R>> cmd = queue.take();
//                logger.info("dequeued: "+cmd);
                cmd.accept(s);
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
                throw new RuntimeException(e);
            }
        });
    }

    private Runnable run() {
        return () -> {
            publisher.subscribe(this);
        };
    }

    @Override
    public void onSubscribe(Subscription s) {
        this.subscription = s;
        subscription.request(Long.MAX_VALUE);
    }

    @Override
    public void onNext(R r) {
//        logger.info("enqueue r: "+r);
        queue.offer(s -> s.next(r));
    }

    @Override
    public void onError(Throwable t) {
        queue.offer(s -> s.error(t));
    }

    @Override
    public void onComplete() {
        queue.offer(s -> s.complete());
    }

    @Override
    public String toString() {
        return "FluxQueueWrapper("+publisher+")";
    }
}
