# Logging and Observability

`event-bus` provides a mechanism for logging its internal operations, which is critical for debugging and analyzing system behavior.

## Connecting a Logger

When creating a bus instance via `make-bus`, you can pass the `:logger` option.

`:logger` is a function that accepts two arguments:
1.  `level` — the logging level (a keyword, e.g., `:info`, `:warn`, `:error`).
2.  `data` — a map with data about the event.

```clojure
(require '[clojure.tools.logging :as log])

(def my-logger
  (fn [level data]
    (case level
      :info  (log/info data)
      :warn  (log/warn data)
      :error (log/error data))))

(def bus (bus/make-bus {:logger my-logger}))
```

## Log Structure

The bus generates several types of events that are sent to the logger. All of them contain an `:event` key describing the record type.

### `:event-published`
- **Level:** `:info`
- **When:** Generated every time `publish` is called.
- **Data:** Contains the full `envelope` of the published message.
- **Usefulness:** Allows you to see absolutely all events passing through the bus.

### `:schema-validation-failed`
- **Level:** `:warn`
- **When:** If a subscriber uses `:schema` and the `payload` fails validation.
- **Data:** Includes `event-type`, `correlation-id`, `payload`, and `:errors` with validation failure details from `malli`.
- **Usefulness:** Helps to quickly identify components that are sending data in the wrong format.

### `:handler-failed`
- **Level:** `:error`
- **When:** If an exception occurs during the execution of a handler function.
- **Data:** Includes the `:exception` with the exception object.
- **Usefulness:** Isolates and reports errors in subscribers without stopping the entire bus.

### `:buffer-full`
- **Level:** `:error`
- **When:** In `:buffered` mode, if the event queue is full and a new event cannot be accepted.
- **Usefulness:** Signals system overload (backpressure).

### `:bus-closing`, `:bus-closed`, `:shutdown-timeout`
- **Level:** `:info` or `:warn`
- **When:** Generated during the graceful shutdown of the bus via `close`.
- **Usefulness:** Help to track the lifecycle of the bus.
