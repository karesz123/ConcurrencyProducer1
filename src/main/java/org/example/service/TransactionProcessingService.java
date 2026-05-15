package org.example.service;

import org.example.model.TransactionEvent;

import java.util.Collection;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.IntStream;

public class TransactionProcessingService {

    private volatile boolean isShutdown = false;
    private final BlockingQueue<TransactionEvent> queue;
    private final ExecutorService consumerPool;
    private final ConcurrentHashMap<String, ReentrantLock> locks = new ConcurrentHashMap<>();
    private final Consumer<TransactionEvent> producerHook;
    private final Consumer<TransactionEvent> consumerHook;

    public TransactionProcessingService() {
        this(true);
    }

    public TransactionProcessingService(boolean startWorker) {
        this(startWorker,
                4,
                500,
                event-> {},
                event-> {});
    }

    public TransactionProcessingService(boolean startWorker,
                                        int workerThreads,
                                        int queueCapacity,
                                        Consumer<TransactionEvent> producerHook,
                                        Consumer<TransactionEvent> consumerHook){
        this.queue = new ArrayBlockingQueue<>(queueCapacity);
        this.consumerPool = Executors.newFixedThreadPool(workerThreads);
        this.producerHook = producerHook;
        this.consumerHook = consumerHook;
        if (startWorker) {
            startConsumers(workerThreads);
        }
    }

    private void startConsumers(int workerThreads) {
        IntStream.range(0, workerThreads)
                .forEach(i -> consumerPool.submit(() -> {
                    while (!Thread.currentThread().isInterrupted()) {
                        try {
                            TransactionEvent event = queue.poll(100, TimeUnit.MILLISECONDS);
                            if (event == null) {
                                if (isShutdown && queue.isEmpty()) {
                                    break;
                                }
                                continue;
                            }
                            consume(event);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                }));
    }

    private void consume(TransactionEvent event) throws InterruptedException {
        consumerHook.accept(event);
        System.out.println("Processing " + event.txId() + ": " + event.payload());
        Thread.sleep(100);
    }

    public void process(TransactionEvent event) {
        if (isShutdown) {
            throw new RejectedExecutionException("Service is shutting down");
        }
        ReentrantLock lock = locks.computeIfAbsent(event.txId(), k -> new ReentrantLock(true));
        lock.lock();
        try {
            producerHook.accept(event);
            queue.put(event);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        finally {
            lock.unlock();
        }
    }

    public int getQueueSize() {
        return queue.size();
    }

    public Collection<TransactionEvent> getQueue() {
        return queue;
    }

    public ThreadPoolExecutor getConsumerPool() {
        return (ThreadPoolExecutor) consumerPool;
    }

    public void shutdown() {
        isShutdown = true;
        consumerPool.shutdown();
    }
}
