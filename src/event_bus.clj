(ns event-bus
  (:require [event-bus-dispatcher :as dispatcher]
            [event-bus-persistence :as persistence]
            [event-bus-persistence-datahike :as datahike-store]
            [event-bus-runtime :as runtime]
            [malli.core :as m]
            [malli.error :as me])
  (:import [java.util UUID]
           [java.util.concurrent Executors ExecutorService TimeUnit ArrayBlockingQueue]))

;; ============================
;; Private Helpers
;; ============================

(defn- ensure-module! [module]
  (when (nil? module)
    (throw (IllegalArgumentException. "Missing required :module in publish options."))))

(defn- make-envelope
  "Creates a root envelope for a new event chain."
  [event-type payload {:keys [correlation-id schema-version module]
                       :or   {schema-version "1.0"}}]
  {:message-id      (UUID/randomUUID)
   :correlation-id  (or correlation-id (UUID/randomUUID))
   :causation-path  []
   :message-type    event-type
   :module          module
   :schema-version  schema-version
   :payload         payload})

(defn- derive-envelope
  "Creates a child envelope based on a parent, enabling causality tracking."
  [parent-envelope new-event-type new-payload {:keys [max-depth module] :as _bus-opts}]
  (let [new-causation-path (conj (:causation-path parent-envelope)
                                 [(:module parent-envelope) (:message-type parent-envelope)])]
    (when (some #(= % [module new-event-type]) new-causation-path)
      (throw (IllegalStateException.
               (str "Cycle detected: event " new-event-type " already in causation path for module " module "."))))
    (when (and max-depth (> (count new-causation-path) max-depth))
      (throw (IllegalStateException.
               (str "Max depth exceeded: " (count new-causation-path) " > " max-depth))))
    (assoc (make-envelope new-event-type new-payload
                          {:correlation-id (:correlation-id parent-envelope)
                           :module module})
           :causation-path new-causation-path)))


;; ============================
;; Bus Constructor
;; ============================

(defn make-bus
  "Creates a new event bus instance.
   
   Options:
   :mode - :unlimited (default) or :buffered.
   :max-depth - Max event chain depth (default: 20).
   :logger - A function `(fn [level data])` for observability.
   
   Options for :buffered mode:
   :buffer-size - Queue size (default: 1024).
   :concurrency - Number of consumer threads (default: 4)."
  [& {:keys [mode max-depth durable persistence
             dispatcher-enabled dispatcher-interval-ms
             dispatcher-batch-size dispatcher-max-attempts]
      :or   {mode :unlimited
             max-depth 20
             dispatcher-enabled true
             dispatcher-interval-ms 500
             dispatcher-batch-size 100
             dispatcher-max-attempts 5}
      :as   opts}]
  (let [store (or persistence
                  (when durable
                    (datahike-store/make-store durable)))
        base-opts (merge {:mode mode
                          :max-depth max-depth
                          :durable durable
                          :dispatcher-enabled dispatcher-enabled
                          :dispatcher-interval-ms dispatcher-interval-ms
                          :dispatcher-batch-size dispatcher-batch-size
                          :dispatcher-max-attempts dispatcher-max-attempts}
                         opts)
        bus-map {:listeners (atom {})
                 :closed?   (atom false)
                 :persistence store
                 :opts      base-opts}
        bus (case mode
              :unlimited
              (assoc bus-map :executor (Executors/newVirtualThreadPerTaskExecutor))

              :buffered
              (let [{:keys [buffer-size concurrency]
                     :or   {buffer-size 1024, concurrency 4}} opts
                    queue (ArrayBlockingQueue. buffer-size)
                    executor (Executors/newVirtualThreadPerTaskExecutor)
                    consumers (doall
                                (repeatedly concurrency
                                            #(.submit ^ExecutorService executor
                                                      ^Runnable
                                                      (fn []
                                                        (loop []
                                                          (if (.isShutdown executor)
                                                            nil
                                                            (let [result (try
                                                                           (let [task (.take queue)]
                                                                             (when task (task)))
                                                                           ::ok
                                                                           (catch InterruptedException _
                                                                             ::stop))]
                                                              (when (= result ::ok)
                                                                (recur)))))))))]
                (assoc bus-map
                       :executor executor
                       :queue queue
                       :consumers consumers)))]
    (if (and durable store dispatcher-enabled)
      (assoc bus :dispatcher (dispatcher/start-dispatcher bus store (:opts bus)))
      bus)))

;; ============================
;; Public API
;; ============================

(defn subscribe
  "Subscribes a handler to an event type.
   The handler function must have the signature `(fn [bus envelope])`."
  [bus event-type handler & {:keys [schema meta inbox]}]
  (runtime/ensure-not-closed! bus)
  (when inbox
    (when-not (:persistence bus)
      (throw (IllegalStateException. "Inbox requires persistence store."))))
  (swap! (:listeners bus) update event-type conj
         (cond-> {:handler handler}
           schema (assoc :schema schema)
           meta (assoc :meta meta)
           inbox (assoc :inbox true)))
  bus)

(defn publish
  "Publishes an event.
   To derive an event from another, pass the parent as `:parent-envelope` in opts.
   Returns the created event envelope."
  [bus event-type payload & [opts]]
  (runtime/ensure-not-closed! bus)
  (let [module (:module opts)
        _ (ensure-module! module)
        schema-version (or (:schema-version opts) "1.0")
        parent-envelope (:parent-envelope opts)
        envelope (if parent-envelope
                   (derive-envelope parent-envelope event-type payload (merge (:opts bus)
                                                                            {:module module
                                                                             :schema-version schema-version}))
                   (make-envelope event-type payload (merge (:opts bus)
                                                            {:module module
                                                             :schema-version schema-version})))
        registry (-> bus :opts :schema-registry)
        event-schemas (get registry event-type)
        publish-schema (get event-schemas schema-version)]
    (when-not publish-schema
      (runtime/log! bus :error {:event :publish-schema-missing
                                :event-type event-type
                                :schema-version schema-version})
      (throw (IllegalStateException.
               (str "Missing schema for event " event-type " version " schema-version))))
    (when-not (m/validate publish-schema payload)
      (runtime/log! bus :error {:event :publish-schema-validation-failed
                                :event-type event-type
                                :schema-version schema-version
                                :correlation-id (:correlation-id envelope)
                                :payload payload
                                :errors (me/humanize (m/explain publish-schema payload))})
      (throw (IllegalStateException.
               (str "Publish schema validation failed for event " event-type " version " schema-version))))
    (runtime/log! bus :info {:event :event-published
                             :envelope envelope})
    (if-let [store (:persistence bus)]
      (if (:durable (:opts bus))
        (do
          (persistence/persist-message! store envelope)
          (runtime/log! bus :info {:event :event-persisted
                                   :message-id (:message-id envelope)
                                   :message-type (:message-type envelope)}))
        (runtime/deliver! bus envelope))
      (runtime/deliver! bus envelope))
    envelope))

(defn unsubscribe
  "Unsubscribes a handler by its reference or metadata."
  [bus event-type matcher]
  (runtime/ensure-not-closed! bus)
  (swap! (:listeners bus) update event-type
         (fn [handlers]
           (remove (fn [{:keys [handler meta]}]
                     (or (= handler matcher)
                         (= meta matcher)))
                   handlers)))
  bus)

(defn clear-listeners
  "Clears all listeners for an event type or for the entire bus."
  ([bus]
   (reset! (:listeners bus) {})
   bus)
  ([bus event-type]
   (swap! (:listeners bus) dissoc event-type)
   bus))

;; ============================
;; Shutdown
;; ============================

(defn close
  "Closes the bus and shuts down its thread pool.
   Options:
   :timeout - Graceful shutdown timeout in ms (default: 10000)."
  ([bus]
   (close bus {:timeout 10000}))
  ([bus {:keys [timeout] :or {timeout 10000}}]
   (when (compare-and-set! (:closed? bus) false true)
     (runtime/log! bus :info {:event :bus-closing})
     (when-let [disp (:dispatcher bus)]
       (dispatcher/stop-dispatcher disp))
     (let [executor ^ExecutorService (:executor bus)]
       (.shutdown executor)
       (when-let [consumers (:consumers bus)]
         (run! #(.cancel % true) consumers))
       (try
         (when-not (.awaitTermination executor timeout TimeUnit/MILLISECONDS)
           (runtime/log! bus :warn {:event :shutdown-timeout}))
         (catch InterruptedException _
           (.interrupt (Thread/currentThread))))
        (when-let [store (:persistence bus)]
         (when (instance? event_bus_persistence_datahike.DatahikeStore store)
           (datahike-store/close-store! store)))
       (runtime/log! bus :info {:event :bus-closed})))))

;; ============================
;; Utility Functions
;; ============================

(defn listener-count
  "Returns the number of listeners for an event type or for the entire bus."
  ([bus]
   (apply + (map count (vals @(:listeners bus)))))
  ([bus event-type]
   (count (get @(:listeners bus) event-type []))))
