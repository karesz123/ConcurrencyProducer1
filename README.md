# ConcurrencyProducer

A concurrent transaction event processing service built in Java, demonstrating producer-consumer patterns, per-key locking, backpressure, and graceful shutdown.

---

## Features

- **Sequential processing per `transactionId`** — events with the same ID are processed one at a time, in the order they arrive
- **Parallel processing across different `transactionId`s** — events with different IDs are processed concurrently
- **Backpressure** — bounded queue rejects new submissions when the service is overloaded
- **Graceful shutdown** — stops accepting new events, but allows all in-flight and queued events to finish before terminating

---

## Architecture

```
producer threads
      │
      ▼
process(event)
      │
      ├── isShutdown check → RejectedExecutionException if shutting down
      │
      ├── per-txId ReentrantLock (ConcurrentHashMap<txId, ReentrantLock>)
      │       └── same txId → sequential insertion ✅
      │       └── different txId → parallel insertion ✅
      │
      └── ArrayBlockingQueue (bounded)
                │
                ▼
        ThreadPoolExecutor (consumer pool)
                │
                ├── worker-1 ──► consume(event)
                ├── worker-2 ──► consume(event)
                ├── worker-3 ──► consume(event)
                └── worker-4 ──► consume(event)
```

---

## Key Design Decisions

### Per-txId Locking
```java
ReentrantLock lock = locks.computeIfAbsent(event.txId(), k -> new ReentrantLock(true));
```
- `ConcurrentHashMap.computeIfAbsent()` is atomic — no duplicate locks for the same `txId`
- `ReentrantLock(true)` is a **fair lock** — threads acquire in arrival order
- Different `txId`s have different locks — they never block each other

### Bounded Queue (Backpressure)
```java
private final BlockingQueue<TransactionEvent> queue = new ArrayBlockingQueue<>(capacity);
```
- `queue.put()` blocks the producer when the queue is full — applies backpressure upstream
- Prevents unbounded memory growth under load

### Graceful Shutdown
```java
public void shutdown() {
    isShutdown = true;      // stop accepting new events
    consumerPool.shutdown(); // signal workers to stop after draining the queue
}
```
- Workers use `queue.poll(100ms)` — they check `isShutdown && queue.isEmpty()` and exit naturally
- No events are lost — the queue is fully drained before workers terminate

---

## Usage

```java
TransactionProcessingService service = new TransactionProcessingService();

// Submit events from multiple threads
service.process(new TransactionEvent("TX-1", "payload-a"));
service.process(new TransactionEvent("TX-2", "payload-b"));
service.process(new TransactionEvent("TX-1", "payload-c")); // sequential with payload-a

// Graceful shutdown — waits for all queued events to finish
service.shutdown();
```

---

## Configuration

```java
new TransactionProcessingService(
    true,       // start consumer workers
    4,          // number of worker threads
    500,        // bounded queue capacity
    event -> {}, // producer hook (runs inside the lock, for testing)
    event -> {}  // consumer hook (runs inside consume(), for testing)
);
```

---

## Tests

| Test | What it proves |
|---|---|
| `givenSingleThreadWhenProcessCalledThenEventIsAddedToQueue` | Basic insertion works |
| `givenSingleThreadWhenProcessCalledMultipleTimesThenEventsAreQueuedInInsertionOrder` | Single-thread insertion order is preserved |
| `givenMultipleThreadsWhenProcessCalledThenAllEventsAreEnqueued` | No events lost under concurrency |
| `givenDifferentTxIdsWhenProcessCalledConcurrentlyThenTheyFinishAlmostSimultaneously` | Different txIds are not blocked by each other (producer side) |
| `givenSameTxIdWhenTwoThreadsCompeteThenSecondIsBlockedUntilFirstCompletes` | Same txId blocks second thread until first completes |
| `givenDifferentTxIdsWhenMultiThreadedConsumerThenProcessedInParallel` | Different txIds are consumed in parallel (consumer side) |
| `givenFullQueueWhenProcessCalledThenProducerIsBlocked` | Producer blocks when queue is full — backpressure works |
| `givenEmptyQueueWhenShutdownThenWorkersTerminate` | Workers terminate cleanly on shutdown |
| `givenNonEmptyQueueWhenShutdownThenAllItemsAreProcessedAndWorkersTerminate` | Workers drain the queue after shutdown before terminating |

### Run tests
```bash
./gradlew test
```

---

## Requirements

- Java 21+
- Gradle

---

## Build

```bash
./gradlew build
```

