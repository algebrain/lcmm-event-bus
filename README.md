# event-bus

A lightweight, in-process event bus for Clojure, designed for loosely coupled modular monoliths.

This event bus enables robust communication between different parts of an application without creating direct dependencies. It is built with causality control and schema validation in mind.

## Core Concepts

- **Separation of Concerns:** The bus facilitates a clean separation between a synchronous "Critical Path" for core business logic and an asynchronous "Reactive Path" for side effects like notifications, logging, or auditing.
- **Causality Control:** The bus automatically tracks the chain of events that led to the creation of a new event. This allows it to detect and prevent execution cycles and excessively long event chains.
- **Schema Validation:** By leveraging `malli`, the bus can validate event payloads at the subscriber level, ensuring data integrity throughout the system.

For a deeper dive into the architectural principles, see [ARCH.md](docs_en/ARCH.md).

## Getting Started

### Testing

The project comes with a comprehensive test suite. To run the tests, execute the following command:

```shell
clj -M:test
```

The test configuration is located in `tests.edn`.

## API Documentation

For detailed API documentation, function signatures, and usage examples, please see **[BUS.md](doc_ens/BUS.md)**.
