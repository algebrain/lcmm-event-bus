# Requirements for `event-bus`

## 1. Message as an "Envelope"
The bus operates on immutable structures (Map) with a mandatory set of metadata:

- `MessageID`: A unique GUID for each message (for deduplication).
- `CorrelationID`: An end-to-end identifier for the entire business chain (Saga).
- `CausationPath`: A set of event types that caused the current message.
- `SchemaVersion`: The data structure version for seamless component updates.
- `MessageType`: The type of the current message, which is recorded in the `CausationPath` when a derived message is created.

## 2. Usage of `CorrelationID`, `CausationPath`, and `MessageType`
These fields must be used "under the hood" when generating a new message based on a received one.

## 3. Causality Control
The bus is a "smart" filter that checks every message before publication:

- **Cycle Detection:** If the type of a new event is already present in the `CausationPath`, the bus throws an exception (Fail-fast).
- **Hop Limit (TTL):** Forcibly stops the chain if the `CausationPath` stack exceeds a specified threshold (e.g., 20 steps).

## 4. "Unlimited Async" Mode (default)
The primary mode of operation, focused on maximum responsiveness:

- Each message immediately spawns a new Virtual Thread (requires Java 21+).
- Threads do not block each other. If one handler is "stuck" on I/O, the others continue to work.

## 5. "Buffered Async" Mode (optional)
A mode to protect the system from overloads (Backpressure):

- Messages do not create a thread immediately but are placed in an `ArrayBlockingQueue` of a specified size.
- A fixed pool of virtual threads retrieves messages from this queue.
- If the queue is full, the bus throws an exception.

**NOTE:** A synchronous mode should be completely removed from the bus code. (This requirement has been met).

## 6. Strict Contracts (Malli)
The bus acts as a "customs office" for data: schema validation on input (`publish`) and on output (`subscribe`). This guarantees that there is no "garbage" data in the bus with broken keys or types.

## 7. Observability
The ability to reconstruct the course of events:

- Logging of each step, preserving the `CorrelationID`.
- The ability to build a call graph (who caused whom) based on the `CausationPath`.
