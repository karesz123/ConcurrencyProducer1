The Problem: TransactionProcessingService
You need to build an in‑memory service that processes incoming transaction events.
Each event has a String transactionId and a String payload.
The service must:

Process events sequentially per transactionId — events with the same ID must be processed one at a time, in the order they arrive.

Process different transactionIds in parallel — events with different IDs can be processed concurrently.

Handle backpressure — if the service is overloaded, new submissions should not be accepted indefinitely. Use a bounded queue and a thread pool.

Graceful shutdown — calling shutdown() should stop accepting new events, but allow all in‑flight and queued events to finish.

For simplicity, "processing" just means printing the event to System.out and sleeping for a short random time (simulate work).