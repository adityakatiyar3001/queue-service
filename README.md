---

# Message Queues

This project is an implementation of a message queue with basic features, now supporting both in-memory and Redis-backed priority queue implementations.

## Background

Message queues are a ubiquitous mechanism for achieving horizontal scalability. However, many production message services (e.g., Amazon's SQS) do not come with an offline implementation suitable for local development and testing. The purpose of this project is to resolve this deficiency by designing a simple message-queue API that supports multiple implementations:

- An in-memory queue, suitable for same-JVM producers and consumers. The in-memory queue is thread-safe.
- A Redis-backed priority queue, which supports both local and distributed producers and consumers. The Redis implementation ensures persistence and reliability across multiple machines.
- A file-based queue, suitable for same-host producers and consumers but potentially different JVMs. The file-based queue is thread-safe and inter-process safe when run in a *nix environment.
- An adapter for a production queue service, such as SQS.

The intended usage is that application components be written to use queues via the common interface (`QueueService`), and injected with an instance suitable for the environment in which that component is running (development, testing, integration-testing, staging, production, etc.).

## Main Features of Message Queue

- **Multiplicity**:  
  A queue supports many producers and many consumers.

- **Delivery**:  
  A queue strives to deliver each message exactly once to exactly one consumer but guarantees at-least-once delivery (it can re-deliver a message to a consumer or deliver a message to multiple consumers in some cases).

- **Order**:  
  A queue strives to deliver messages in FIFO order for the in-memory implementation, while the Redis-backed priority queue delivers messages based on custom priorities (higher-priority messages are delivered first).

- **Reliability**:  
  When a consumer receives a message, it is not removed from the queue. Instead, it is temporarily suppressed (becomes "invisible"). If the consumer that received the message does not subsequently delete it within a timeout period (the "visibility timeout"), the message automatically becomes visible at the head of the queue again, ready to be delivered to another consumer.

## New Redis-based Priority Queue Implementation

### Redis Priority Queue (`RedisPriorityQueueService`)

This implementation uses Redis as the backend to support a **priority queue**. The queue allows you to assign priorities to messages, ensuring that higher-priority messages are processed first. Redis sorted sets are used to maintain the priority order.

- **Priority-based ordering**: Unlike the in-memory or file-based queue that uses FIFO order, this implementation allows you to specify message priority. Messages with higher priority are dequeued first.
  
- **Persistence**: Messages are persisted in Redis, which ensures durability across different producers and consumers, even on different machines.

- **Visibility Timeout**: Similar to other queue implementations, the visibility timeout is supported. Messages are temporarily invisible once pulled and can be re-delivered if not deleted within the timeout.

### Key Features of `RedisPriorityQueueService`

- **push(queueUrl, msgBody, priority)**: Pushes a message into the Redis priority queue with the specified priority. Higher-priority messages are delivered first.
  
- **pull(queueUrl)**: Retrieves the highest-priority message from the queue, marks it invisible, and updates metadata like the `receiptId` and `timestamp`.

- **delete(queueUrl, receiptId)**: Deletes the message with the corresponding receipt ID from Redis.

- **Message Updates**: After pulling a message, the message's metadata (like `receiptId`, timestamp, and attempts) is updated and re-stored in Redis.

## Code Structure

The code is structured under the `com.example` package with the following components:

1. **`QueueService.java`**:  
   The interface that defines essential queue actions:
   - `push(String queueUrl, String messageBody, int priority)`: Pushes a message onto the specified queue with an optional priority.
   - `pull(String queueUrl)`: Retrieves the highest-priority message from the queue.
   - `delete(String queueUrl, String receiptId)`: Deletes a received message from the queue using the receipt ID.

2. **`InMemoryQueueService.java`**:  
   An in-memory version of `QueueService`. This queue is thread-safe and suitable for local development where durability is not a concern.

3. **`RedisPriorityQueueService.java`**:  
   A Redis-backed priority queue implementation that allows for persistence and ordering of messages based on custom priorities. This service leverages Redis sorted sets to store and retrieve messages with the highest priority.

4. **`FileQueueService.java`**:  
   Implements a file-based version of `QueueService`, which uses the file system to coordinate between producers and consumers in different JVMs. It is thread-safe and inter-process safe when used in *nix environments.

5. **`SqsQueueService.java`**:  
   An adapter for Amazon SQS that allows interaction with production-grade queue services.

6. **Config file**:  
   The configuration file (`config.properties`) is located under `src/main/resources` and contains necessary properties such as Redis connection details and visibility timeout.

7. **Unit tests**:  
   Unit tests are provided to verify the behavior of the message queue implementations, including handling of the visibility timeout and message ordering based on priorities.

## Redis Integration

The Redis integration is handled within `RedisPriorityQueueService`. It uses the following Redis commands:

- **`zadd`**: Adds a message to a Redis sorted set with a score calculated based on message priority and timestamp.
- **`zrangeWithScores`**: Retrieves the highest-priority message.
- **`zrem`**: Removes a message from the sorted set after processing.

The Redis connection details, such as the host and authentication token, are managed through the `config.properties` file.

## Building and Running

You can use Maven to build the project and run tests from the command-line with:

```bash
mvn package
```

### Running Redis-based Tests
To test the `RedisPriorityQueueService`, make sure Redis is running and configured correctly in the `config.properties` file. Unit tests will verify the functionality of the Redis-backed queue.

---
