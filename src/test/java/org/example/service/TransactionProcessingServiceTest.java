package org.example.service;

import org.example.model.TransactionEvent;
import org.junit.jupiter.api.Test;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

public class TransactionProcessingServiceTest {

    @Test
    void givenSingleThreadWhenProcessCalledThenEventIsAddedToQueue() {
        TransactionProcessingService service = new TransactionProcessingService(false); // no worker

        service.process(new TransactionEvent("TX-1", "payload-1"));

        assertThat(service.getQueueSize()).isEqualTo(1);
    }

    @Test
    void givenSingleThreadWhenProcessCalledMultipleTimesThenEventsAreQueuedInInsertionOrder() {
        TransactionProcessingService service = new TransactionProcessingService(false); // no worker
        String txId = "TX-ORDER";
        int count = 100;

        List<String> expectedOrder = IntStream.range(0, count)
                .mapToObj(i -> "event-" + i)
                .peek(payload -> service.process(new TransactionEvent(txId, payload)))
                .toList();

        List<String> actualOrder = service.getQueue().stream()
                .map(TransactionEvent::payload)
                .toList();
        assertThat(actualOrder).isEqualTo(expectedOrder);
    }

    @Test
    void givenMultipleThreadsWhenProcessCalledThenAllEventsAreEnqueued() throws InterruptedException {
        int threadCount = 10;
        int eventsPerThread = 10;
        TransactionProcessingService service = new TransactionProcessingService(false); // no worker
        CountDownLatch latch = new CountDownLatch(1);
        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            final int idx = i;
            Thread t = new Thread(() -> {
                try {
                    latch.await();
                    for (int j = 0; j < eventsPerThread; j++) {
                        service.process(new TransactionEvent("TX-" + idx, "event-" + j));
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            threads.add(t);
            t.start();
        }

        latch.countDown();
        for (Thread t : threads) t.join();

        assertThat(service.getQueueSize()).isEqualTo(threadCount * eventsPerThread);
    }


    @Test
    void givenDifferentTxIdsWhenProcessCalledConcurrentlyThenTheyFinishAlmostSimultaneously() throws InterruptedException {
        TransactionProcessingService service = new TransactionProcessingService(false, 4, 100, event -> {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, event -> {});
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicLong t1End = new AtomicLong();
        AtomicLong t2End = new AtomicLong();

        Thread t1 = new Thread(() -> {
            try {
                startLatch.await();
            } catch (InterruptedException e) {
                return;
            }
            service.process(new TransactionEvent("TX-1", "payload"));
            t1End.set(System.currentTimeMillis());
        });
        Thread t2 = new Thread(() -> {
            try {
                startLatch.await();
            } catch (InterruptedException e) {
                return;
            }
            service.process(new TransactionEvent("TX-2", "payload"));
            t2End.set(System.currentTimeMillis());
        });
        t1.start();
        t2.start();
        startLatch.countDown();
        t1.join();
        t2.join();

        long gap = Math.abs(t2End.get() - t1End.get());
        // With per‑key locks, both threads run concurrently and finish within a few ms of each other.
        // A gap of ≤ 200 ms proves that they did NOT execute sequentially (which would give ~500 ms gap).
        assertThat(gap).isLessThanOrEqualTo(200);
    }

    @Test
    void givenSameTxIdWhenTwoThreadsCompeteThenSecondIsBlockedUntilFirstCompletes() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        ConcurrentMap<Long, String> order = new ConcurrentHashMap<>();

        TransactionProcessingService service = new TransactionProcessingService(
                false,
                1,
                10,
                event -> {
                    try {
                        order.put(Thread.currentThread().threadId(), "Log acquired");
                        latch.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                },
                event -> {}
        );

        Thread threadAcquiredLock = new Thread(() -> service.process(new TransactionEvent("TX-1", "payload")));
        Thread threadWaitingForLock = new Thread(() -> service.process(new TransactionEvent("TX-1", "payload")));
        threadAcquiredLock.setDaemon(true);
        threadWaitingForLock.setDaemon(true);
        threadAcquiredLock.start();
        Thread.sleep(50);
        threadWaitingForLock.start();
        Thread.sleep(50);

        try {
            assertThat(order).hasSize(1);
            assertThat(order.get(threadAcquiredLock.threadId())).isEqualTo("Log acquired");
            assertThat(order.get(threadWaitingForLock.threadId())).isNull();
        } finally {
            latch.countDown(); // always release
        }

    }


    @Test
    void givenDifferentTxIdsWhenMultiThreadedConsumerThenProcessedInParallel() throws Exception {
        AtomicInteger runningThreads = new AtomicInteger(0);
        CountDownLatch started = new CountDownLatch(2);
        CountDownLatch holdBeforeProcessing = new CountDownLatch(1);

        Consumer<TransactionEvent> processor = event -> {
            runningThreads.incrementAndGet();
            started.countDown();
            try {
                holdBeforeProcessing.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };

        TransactionProcessingService service = new TransactionProcessingService(
                true, 4, 10,
                event -> {}, processor);

        service.process(new TransactionEvent("TX-A", "A"));
        service.process(new TransactionEvent("TX-B", "B"));
        boolean reached = started.await(2, TimeUnit.SECONDS);

        assertThat(reached).isTrue();
        assertThat(runningThreads.get()).isEqualTo(2);
        holdBeforeProcessing.countDown();
        service.shutdown();
    }


    @Test
    void givenFullQueueWhenProcessCalledThenProducerIsBlocked() throws InterruptedException {
        int queueCapacity = 10;
        TransactionProcessingService service = new TransactionProcessingService(
                false,
                4,
                queueCapacity,
                event -> {},
                event -> {});

        for (int i = 0; i < queueCapacity; i++) {
            service.process(new TransactionEvent("TX-1", "event-" + i));
        }

        Thread producer = Thread.ofPlatform()
                .daemon()
                .unstarted(() -> service.process(new TransactionEvent("TX-1", "overflow")));

        producer.start();
        Thread.sleep(50);

        assertThat(producer.getState()).isIn(Thread.State.WAITING, Thread.State.TIMED_WAITING);
        assertThat(service.getQueueSize()).isEqualTo(queueCapacity);
        producer.interrupt();
    }

    @Test
    void givenEmptyQueueWhenShutdownThenWorkersTerminate() throws InterruptedException {
        TransactionProcessingService service = new TransactionProcessingService(true,
                4,
                100,
                event -> {},
                event -> {});

        service.shutdown();
        Thread.sleep(200);

        assertThat(service.isConsumerPoolTerminated()).isTrue();
    }

    @Test
    void givenNonEmptyQueueWhenShutdownThenAllItemsAreProcessedAndWorkersTerminate() throws InterruptedException {
        CountDownLatch holdConsumers = new CountDownLatch(1);
        TransactionProcessingService service = new TransactionProcessingService(
                true, 4, 100,
                event -> {},
                event -> {
                    try {
                        holdConsumers.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
        );
        for (int i = 0; i < 10; i++) {
            service.process(new TransactionEvent("TX-1", "event-" + i));
        }

        service.shutdown();
        Thread.sleep(50);

        assertThat(service.getConsumerPoolActiveCount()).isGreaterThan(0);

        holdConsumers.countDown();
        Thread.sleep(2000);

        assertThat(service.isConsumerPoolTerminated()).isTrue();
    }

    @Test
    void givenTwoTxIdsWhenProcessCalledThenSameTxIdIsSequentialAndDifferentTxIdsAreParallel() throws InterruptedException {
        int eventsPerTx = 3;
        CountDownLatch allDone = new CountDownLatch(eventsPerTx * 2);
        ConcurrentHashMap<String, AtomicInteger> activeCounts = new ConcurrentHashMap<>();
        AtomicInteger concurrentSameTxId = new AtomicInteger(0);
        AtomicLong maxEndTime = new AtomicLong(0);

        TransactionProcessingService service = new TransactionProcessingService(
                true, 4, 100,
                event -> {},
                event -> {
                    AtomicInteger active = activeCounts.computeIfAbsent(event.txId(), k -> new AtomicInteger(0));
                    if (active.incrementAndGet() > 1) {
                        concurrentSameTxId.incrementAndGet();
                    }
                    try { Thread.sleep(50); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                    active.decrementAndGet();
                    maxEndTime.updateAndGet(prev -> Math.max(prev, System.currentTimeMillis()));
                    allDone.countDown();
                }
        );

        long start = System.currentTimeMillis();
        for (int i = 0; i < eventsPerTx; i++) {
            service.process(new TransactionEvent("TX-1", "event-" + i));
            service.process(new TransactionEvent("TX-2", "event-" + i));
        }

        allDone.await(5, TimeUnit.SECONDS);
        long elapsed = maxEndTime.get() - start;


        assertThat(elapsed).isLessThan(500);
        assertThat(concurrentSameTxId.get()).isEqualTo(0);
        service.shutdown();
    }

}
