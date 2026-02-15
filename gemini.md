# Gemini Project Context: event-bus

## Project Overview

This project is a Clojure implementation of an event bus. The architecture is a "Loosely Coupled Modular Monolith" as defined in `ARCH.md`.

### Key Architectural Principles:

- **Separation of Concerns:** The system distinguishes between a synchronous "Critical Path" for business-critical DB transactions and an asynchronous "Reactive Path" for side effects (notifications, logging, etc.) handled by the event bus.
- **Loose Coupling:** Modules communicate exclusively through immutable messages (Envelopes) on the bus. No direct function calls between modules.
- **Causality & Control:** The bus is responsible for tracking the chain of events (`CausationPath`), tracing them (`CorrelationID`), and preventing execution loops and excessive event depth.

## Core Files

- `ARCH.md`: Defines the high-level architectural style.
- `REQUIREMENTS.md`: Lists the specific functional requirements for the bus.
- `deps.edn`: Clojure project dependencies, including `malli` for schema validation.
- `src/event_bus.clj`: The main implementation of the event bus.
- `test/event_bus_test.clj`: Tests for the event bus.
- `tests.edn`: Test runner (`kaocha`) configuration.

## Implementation Summary (`src/event_bus.clj`)

The current implementation in `event_bus.clj` provides a fully featured event bus that aligns with the project's requirements.

- **`make-bus`**: The constructor. Supports `:unlimited` and `:buffered` async modes and accepts an optional `:logger` function for observability.
- **`subscribe`**: Subscribes a handler to an event. The handler function must have the signature `(fn [bus envelope])` to allow it to publish derived events.
- **`publish`**: The main function for publishing events. It returns the created event `envelope`, allowing the caller to retrieve the `correlation-id` for tracking.
    - It supports creating both root events and derived events.
    - To create a derived event, the caller passes the original message as `:parent-envelope` in the options map. This activates the causality tracking mechanisms (cycle/depth detection).
- **Error Isolation**: Subscriber handlers are wrapped in a `try/catch` block to ensure that an error in one does not affect others.
- **Schema Validation**: Uses `malli` to validate event payloads against schemas defined at the subscriber level.

## Project Status

The project is a complete and tested Clojure implementation of an event bus, conforming to the architecture defined in `ARCH.md`.

### Key Features:
- **Core Logic:** The implementation resides in `src/event_bus.clj`.
- **Causality Control:** The bus correctly tracks event chains, detects cycles, and enforces max depth.
- **Async Modes:** Supports both `:unlimited` and `:buffered` asynchronous processing.
- **Schema Validation:** Uses `malli` to validate event payloads.
- **Testing:** A comprehensive test suite is in `test/event_bus_test.clj` and is configured to run via `clj -M:test` (using `tests.edn`).

The project structure and naming have been finalized, and all tests are passing. The system is in a stable, complete state.