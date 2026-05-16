package org.example.service;

import org.example.model.TransactionEvent;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

public class TransactionProcessingService {

    private volatile boolean isShutdown = false;
    private final BlockingQueue<TransactionEvent> queue;
    private final ExecutorService dispatcher;
    private final ThreadPoolExecutor[] txWorkers;
    private final ConcurrentHashMap<String, ReentrantLock> locks = new ConcurrentHashMap<>();
    private final Consumer<TransactionEvent> producerHook;
    private final Consumer<TransactionEvent> consumerHook;

    public TransactionProcessingService() {
        this(true);
    }

    public TransactionProcessingService(boolean startWorker) {
        this(startWorker, 4, 500, event -> {}, event -> {});
    }

    public TransactionProcessingService(boolean startWorker,
                                        int workerThreads,
                                        int queueCapacity,
                                        Consumer<TransactionEvent> producerHook,
                                        Consumer<TransactionEvent> consumerHook) {
        this.queue = new ArrayBlockingQueue<>(queueCapacity);
        this.dispatcher = Executors.newSingleThreadExecutor();
        this.txWorkers = new ThreadPoolExecutor[workerThreads];
        for (int i = 0; i < workerThreads; i++) {
            txWorkers[i] = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        }
        this.producerHook = producerHook;
        this.consumerHook = consumerHook;
        if (startWorker) {
            startConsumers();
        }
    }

    private void startConsumers() {
        dispatcher.submit(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    TransactionEvent event = queue.poll(100, TimeUnit.MILLISECONDS);
                    if (event == null) {
                        if (isShutdown && queue.isEmpty()) {
                            for (ExecutorService w : txWorkers) {
                                w.shutdown();
                            }
                            break;
                        }
                        continue;
                    }
                    int index = Math.abs(event.txId().hashCode() % txWorkers.length);
                    txWorkers[index].submit(() -> {
                        try {
                            consume(event);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    });
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
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
        } finally {
            lock.unlock();
        }
    }

    public int getQueueSize() {
        return queue.size();
    }

    public Collection<TransactionEvent> getQueue() {
        return queue;
    }

    public boolean isConsumerPoolTerminated() {
        return Arrays.stream(txWorkers).allMatch(ExecutorService::isTerminated);
    }

    public long getConsumerPoolActiveCount() {
        return Arrays.stream(txWorkers)
                .mapToLong(ThreadPoolExecutor::getActiveCount)
                .sum();
    }

    public void shutdown() {
        isShutdown = true;
        dispatcher.shutdown();
    }
}
