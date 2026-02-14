# Architecture: Loosely Coupled Modular Monolith

This document defines the architectural style of a system built on top of `event-bus`.

## 1. Separation of Concerns (CQRS-like)

The system is divided into two logical flows:

- **The Critical Path (Synchronous):**
  - Responsible for executing key business transactions that must be atomic and fast (e.g., creating an order in the DB).
  - This path **must not** contain slow operations (HTTP requests, sending emails, complex data processing).
  - On success, it publishes an event to the bus about the result (e.g., `:order/created`).

- **The Reactive Path (Asynchronous):**
  - Represented by event handlers subscribed to the bus.
  - Executes all "side effects": sending notifications, logging, updating search indexes, calling external systems.
  - Works asynchronously and does not block the critical path. An error in one handler does not affect others or the main transaction.

## 2. Loose Coupling

- System modules **never** call each other's functions directly.
- The only method of communication is through asynchronous, immutable messages (`envelopes`) on the `event-bus`.
- Module A does not know about the existence of Module B. It simply publishes an `:event-from-a`, and Module B reacts to it. This allows for adding, removing, or changing modules without affecting the rest of the system.

## 3. Causality & Observability

- **Causality:** The bus tracks which event caused which, using `CausationPath` and `CorrelationID`. This allows for:
  - Easily tracing the entire chain of calls that led to an error.
  - Automatically preventing event cycles (when `A` causes `B`, and `B` causes `A` again).
- **Observability:** Every event can be logged, providing a complete picture of what is happening in the system.
