# API Documentation: event-bus

This document describes the public API for `event-bus`.

## Constructor

### `make-bus`

Creates a new bus instance.

```clojure
(require '[event-bus :as bus])

(def a-bus (bus/make-bus))
```

#### Options

You can pass an options map for configuration:

- `:mode`: The mode of operation.
  - `:unlimited` (default): Each handler is executed in a new virtual thread.
  - `:buffered`: Events are placed in a fixed-size queue, from which a limited pool of threads processes them.
- `:max-depth`: Maximum depth of the event chain (default: `20`).
- `:logger`: A function for logging, which accepts `(fn [level data])`.
- `:buffer-size`: (for `:buffered` mode) The size of the queue (default: `1024`).
- `:concurrency`: (for `:buffered` mode) The number of handler threads (default: `4`).

**Example with options:**
```clojure
(def buffered-bus
  (bus/make-bus {:mode :buffered
                 :buffer-size 500
                 :concurrency 8
                 :logger (fn [lvl d] (println "LOG:" lvl d))}))
```

## Core Functions

### `subscribe`

Subscribes a handler function to a specific event type.

- **Signature:** `(subscribe bus event-type handler & {:keys [schema meta]})`
- **`event-type`:** A keyword identifying the event (e.g., `:order/created`).
- **`handler`:** The function to be called. **Important:** its signature must be `(fn [bus envelope])` to allow it to publish derived events.
- **`:schema`:** (optional) A `malli` schema to validate the event payload. If validation fails, the handler will not be called.

**Example:**
```clojure
(bus/subscribe a-bus
               :user/registered
               (fn [bus envelope]
                 (println "New user:" (:payload envelope))
                 ;; Publish a new event based on the received one
                 (bus/publish bus
                              :email/send-welcome
                              (:payload envelope)
                              {:parent-envelope envelope})))
```

### `publish`

Publishes an event to the bus.

- **Signature:** `(publish bus event-type payload & [opts])`
- **`payload`:** The event data (usually a map).
- **`opts`:** (optional) An options map.

#### Options for `publish`

- `:parent-envelope`: **The key option for causality control.** If you are publishing an event in response to another, pass the original `envelope` here. The bus will automatically extract the `CorrelationID` and update the `CausationPath`.

**Example (root event):**
```clojure
;; Someone logged in
(bus/publish a-bus :user/logged-in {:user-id 123})
```

**Example (derived event):**
```clojure
;; Inside a handler for :user/logged-in
(fn [bus envelope]
  (let [user-id (-> envelope :payload :user-id)]
    (bus/publish bus
                 :audit/user-activity
                 {:activity "login" :user user-id}
                 {:parent-envelope envelope}))) ; <-- Pass the parent
```

### `unsubscribe`

Unsubscribes a handler from an event.

- **Signature:** `(unsubscribe bus event-type matcher)`
- **`matcher`:** Either a direct reference to the handler function or the metadata (`:meta`) that was passed during subscription.

**Example:**
```clojure
(defn my-handler [bus envelope] (println "Called!"))

(bus/subscribe a-bus :my/event my-handler)

;; ...later
(bus/unsubscribe a-bus :my/event my-handler)
```
