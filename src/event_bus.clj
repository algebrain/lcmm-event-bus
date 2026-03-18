(ns event-bus
  (:require [clojure.core.async :as async]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [db.tx-store :as tx-store]
            [malli.core :as m]
            [malli.error :as me])
  (:import [java.util UUID]
           [java.util.concurrent Executors ExecutorService TimeUnit ArrayBlockingQueue]))

;; ============================
;; Private Helpers & Logging
;; ============================

(defn- log!
  "Calls the configured logger, if any."
  [bus level data]
  (when-let [logger (-> bus :opts :logger)]
    (try
      (let [opts (:opts bus)
            component (or (:component opts) :event-bus)
            payload (:payload data)
            payload-size (when (contains? data :payload)
                           (count (.getBytes ^String (pr-str payload) "UTF-8")))
            safe-payload (let [mode (or (:log-payload opts) :none)
                               max-chars (or (:log-payload-max-chars opts) 1024)]
                           (case mode
                             :none nil
                             :keys (if (map? payload) (vec (keys payload)) :non-map)
                             :truncated (let [s (pr-str payload)]
                                          (if (> (count s) max-chars)
                                            (str (subs s 0 max-chars) "...")
                                            s))
                             nil))
            _ (when-let [{:keys [on-events dir max-bytes redact]} (:payload-dump opts)]
                (when (and (set? on-events)
                           (contains? on-events (:event data))
                           (:payload-dump data)
                           dir)
                  (try
                    (let [dir-file (io/file dir)
                          _ (.mkdirs dir-file)
                          sanitized (if redact (redact (:payload-dump data)) (:payload-dump data))
                          dump-map {:event (:event data)
                                    :correlation-id (:correlation-id data)
                                    :event-type (:event-type data)
                                    :payload sanitized}
                          content (pr-str dump-map)
                          max-bytes (or max-bytes (* 1024 1024))
                          bytes (.getBytes ^String content "UTF-8")
                          content (if (> (count bytes) max-bytes)
                                    (let [truncated (subs content 0 (min (count content) max-bytes))]
                                      (str truncated "..."))
                                    content)
                          ts (System/currentTimeMillis)
                          cid (or (:correlation-id data) (UUID/randomUUID))
                          file (io/file dir-file (str "event-bus-" ts "-" cid ".edn"))]
                      (spit file content))
                    (catch Throwable _ nil))))]
        (logger level (cond-> (merge {:component component} data)
                        (contains? data :payload) (assoc :payload safe-payload)
                        payload-size (assoc :payload-size payload-size)
                        true (dissoc :payload-dump))))
      (catch Throwable _
        ;; Never allow logger failures to break the critical path
        nil))))

(defn- ensure-module! [module]
  (when (nil? module)
    (throw (IllegalArgumentException. "Missing required :module in publish options."))))

(defn- current-handlers
  [bus event-type]
  (get @(:listeners bus) event-type []))

(defn- make-envelope
  "Creates a root envelope for a new event chain."
  [event-type payload {:keys [correlation-id schema-version module]
                       :or   {schema-version "1.0"}}]
  {:message-id      (UUID/randomUUID)
   :correlation-id  (or correlation-id (UUID/randomUUID))
   :causation-path  []
   :event-type      event-type
   :module          module
   :schema-version  schema-version
   :payload         payload})

(defn- derive-envelope
  "Creates a child envelope based on a parent, enabling causality tracking."
  [parent-envelope new-event-type new-payload {:keys [max-depth module] :as _bus-opts}]
  (let [new-causation-path (conj (:causation-path parent-envelope)
                                 [(:module parent-envelope) (:event-type parent-envelope)])]
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
;; transact: Internal DB and Worker Helpers
;; ============================

(defn- now-inst []
  (java.util.Date.))

(def ^:private default-tx-retention-ms
  (* 7 24 60 60 1000))

(def ^:private default-tx-cleanup-interval-ms
  (* 60 60 1000))

(defn- ensure-tx-store! [bus]
  (when-not (:tx-store bus)
    (throw (IllegalStateException. "transact requires :tx-store in make-bus options."))))

(defn- find-handler-by-id [listeners event-type handler-id]
  (some (fn [{:keys [id] :as entry}]
          (when (= id handler-id) entry))
        (get listeners event-type [])))

(defn- complete-tx! [bus tx-id ok? error]
  (let [now (now-inst)]
    (tx-store/update-tx! (:tx-store bus) tx-id (if ok? :ok :failed) now))
  (when-let [{:keys [promise chan]} (get @(:tx-results bus) tx-id)]
    (let [result (if ok?
                   {:ok? true :tx-id tx-id}
                   {:ok? false :tx-id tx-id :error error})]
      (deliver promise result)
      (async/put! chan result)
      (swap! (:tx-results bus) dissoc tx-id))))

(defn- update-tx-status! [bus tx-id]
  (let [status (tx-store/tx-status (:tx-store bus) tx-id)]
    (when (#{:ok :failed} status)
      (complete-tx! bus tx-id (= status :ok) (when (= status :failed) :handler-failed)))))

(defn- run-handler-with-timeout
  [handler bus envelope timeout-ms]
  (try
    (deref (future (handler bus envelope))
           timeout-ms
           ::timeout)
    (catch Throwable e
      {:error e})))

(defn- evaluate-handler-result
  [handler-entry payload-value]
  (let [{:keys [handler schema]} handler-entry]
    (if (and schema (not (m/validate schema payload-value)))
      {:status :failed
       :retryable? false
       :error {:event :schema-validation-failed
               :errors (me/humanize (m/explain schema payload-value))}}
      {:status :ok
       :retryable? false
       :handler handler})))

(defn- finalize-handler-result
  [value]
  (cond
    (and (map? value) (:error value))
    {:status :failed
     :retryable? true
     :error {:event :handler-exception
             :exception (:error value)}}

    (= value ::timeout)
    {:status :timeout
     :retryable? true
     :error {:event :handler-timeout}}

    (true? value)
    {:status :ok
     :retryable? false}

    :else
    {:status :failed
     :retryable? true
     :error {:event :handler-returned-false}}))

(defn- process-handler! [bus row]
  (let [[h-eid _msg-eid _tx-eid event-type payload module schema-version
         correlation-id message-id handler-id retry-count] row
        listeners @(:listeners bus)
        handler-entry (find-handler-by-id listeners event-type handler-id)
        now (now-inst)
        max-retries (or (-> bus :opts :handler-max-retries) 3)
        backoff-ms (or (-> bus :opts :handler-backoff-ms) 1000)
        timeout-ms (or (-> bus :opts :tx-handler-timeout) 10000)
        payload-value (if (string? payload) (edn/read-string payload) payload)
        envelope {:message-id message-id
                  :correlation-id correlation-id
                  :causation-path []
                  :event-type event-type
                  :module module
                  :schema-version schema-version
                  :payload payload-value}
        result (if-not handler-entry
                 {:status :failed
                  :retryable? false
                  :error {:event :handler-missing}}
                 (let [pre-result (evaluate-handler-result handler-entry payload-value)]
                   (if (:handler pre-result)
                     (let [value (run-handler-with-timeout (:handler pre-result)
                                                           bus
                                                           envelope
                                                           timeout-ms)]
                       (finalize-handler-result value))
                     pre-result)))
        {:keys [status retryable? error]} result
        next-retry (inc (long retry-count))
        exhausted? (and retryable? (>= next-retry max-retries))
        final-status (cond
                       (= status :ok) :ok
                       exhausted? status
                       retryable? :pending
                       :else status)
        next-at (if (and retryable? (not exhausted?))
                  (java.util.Date. (+ (.getTime now) backoff-ms))
                  now)
        update {:handler-row-id h-eid
                :status final-status
                :retry-count (if (= status :ok) retry-count next-retry)
                :updated-at now
                :next-at next-at}]
    (cond-> update
      error (assoc :last-error (pr-str error)))))

(defn- process-pending-handlers! [bus]
  (let [now (now-inst)
        rows (tx-store/query-pending-handlers (:tx-store bus) now)]
    (doseq [row rows]
      (let [update (process-handler! bus row)
            tx-id (nth row 2)]
        (tx-store/update-handler! (:tx-store bus) update)
        (update-tx-status! bus tx-id)))))

(defn- tx-worker-loop [bus stop-flag]
  (loop []
    (when-not @stop-flag
      (try
        (process-pending-handlers! bus)
        (when-let [store (:tx-store bus)]
          (let [interval-ms (-> bus :opts :tx-cleanup-interval-ms)
                retention-ms (-> bus :opts :tx-retention-ms)
                last-cleanup-ms @(:tx-cleanup-last bus)
                now-ms (System/currentTimeMillis)]
            (when (and interval-ms retention-ms
                       (>= (- now-ms last-cleanup-ms) interval-ms))
              (reset! (:tx-cleanup-last bus) now-ms)
              (try
                (let [deleted (tx-store/cleanup! store (java.util.Date. now-ms) retention-ms)]
                  (log! bus :info {:event :tx-cleanup
                                   :deleted deleted
                                   :retention-ms retention-ms
                                   :cleanup-interval-ms interval-ms}))
                (catch Throwable e
                  (log! bus :error {:event :tx-cleanup-failed
                                    :exception e}))))))
        (catch Throwable e
          (log! bus :error {:event :tx-worker-failed
                            :exception e})))
      (Thread/sleep 50)
      (recur))))

(defn- start-tx-worker! [bus]
  (when (and (:tx-store bus) (nil? @(:tx-worker bus)))
    (let [stop-flag (:tx-stop bus)
          executor (:tx-executor bus)
          worker (.submit ^ExecutorService executor
                          ^Runnable
                          (fn [] (tx-worker-loop bus stop-flag)))]
      (reset! (:tx-worker bus) worker))))

(defn- invoke-handler!
  [bus handler envelope event-type]
  (try
    (handler bus envelope)
    (catch Throwable e
      (log! bus :error {:event :handler-failed
                        :event-type event-type
                        :correlation-id (:correlation-id envelope)
                        :payload-dump (:payload envelope)
                        :exception e}))))

(defn- dispatch-handler-entry-now!
  [bus envelope event-type {:keys [handler schema]}]
  (if-not schema
    (.submit ^ExecutorService (:executor bus)
             ^Runnable #(invoke-handler! bus handler envelope event-type))
    (if (m/validate schema (:payload envelope))
      (.submit ^ExecutorService (:executor bus)
               ^Runnable #(invoke-handler! bus handler envelope event-type))
      (log! bus :warn {:event :schema-validation-failed
                       :event-type event-type
                       :correlation-id (:correlation-id envelope)
                       :payload (:payload envelope)
                       :errors (me/humanize (m/explain schema (:payload envelope)))}))))

(defn- dispatch-event-now!
  [bus envelope handlers]
  (let [event-type (:event-type envelope)]
    (doseq [handler-entry handlers]
      (dispatch-handler-entry-now! bus envelope event-type handler-entry))))

(defn- dispatch-buffered-event!
  [bus envelope]
  (let [event-type (:event-type envelope)
        handlers (current-handlers bus event-type)]
    (doseq [{:keys [handler schema]} handlers]
      (if-not schema
        (invoke-handler! bus handler envelope event-type)
        (if (m/validate schema (:payload envelope))
          (invoke-handler! bus handler envelope event-type)
          (log! bus :warn {:event :schema-validation-failed
                           :event-type event-type
                           :correlation-id (:correlation-id envelope)
                           :payload (:payload envelope)
                           :errors (me/humanize (m/explain schema (:payload envelope)))}))))))

(defn- dispatch-queued-item!
  [bus item]
  (case (:kind item)
    :dispatch-event (dispatch-buffered-event! bus (:envelope item))
    nil))

(defn- buffered-consumer-loop
  [bus]
  (let [executor ^ExecutorService (:executor bus)
        queue ^ArrayBlockingQueue (:queue bus)]
    (loop []
      (if (.isShutdown executor)
        nil
        (let [result (try
                       (let [item (.take queue)]
                         (when item
                           (dispatch-queued-item! bus item)))
                       ::ok
                       (catch InterruptedException _
                         ::stop))]
          (when (= result ::ok)
            (recur)))))))

(defn- start-buffered-consumers!
  [bus]
  (when-let [consumers-atom (:consumers bus)]
    (when (empty? @consumers-atom)
      (let [executor ^ExecutorService (:executor bus)
            concurrency (or (-> bus :opts :concurrency) 4)
            consumers (doall
                       (repeatedly concurrency
                                   #(.submit executor
                                             ^Runnable
                                             (fn []
                                               (buffered-consumer-loop bus)))))]
        (reset! consumers-atom consumers)))))

(defn- enqueue-buffered-event!
  [bus envelope]
  (let [item {:kind :dispatch-event
              :event-type (:event-type envelope)
              :envelope envelope}
        queue ^ArrayBlockingQueue (:queue bus)]
    (when-not (.offer queue item)
      (log! bus :error {:event :buffer-full
                        :event-type (:event-type envelope)
                        :correlation-id (:correlation-id envelope)})
      (throw (IllegalStateException. "Event bus buffer is full.")))))

;; ============================
;; Bus Constructor
;; ============================

(defn make-bus
  "Creates a new event bus instance.
   
   Options:
   :mode - :unlimited (default) or :buffered.
   :max-depth - Max event chain depth (default: 20).
   :logger - A function `(fn [level data])` for observability.
   :tx-retention-ms - Retention for successful transact records (default: 7 days).
   :tx-cleanup-interval-ms - Cleanup interval for successful transact records (default: 1 hour).
   
   Options for :buffered mode:
   :buffer-size - Queue size (default: 1024).
   :concurrency - Number of consumer threads (default: 4)."
  [& {:keys [mode max-depth]
      :or   {mode :unlimited, max-depth 20}
      :as   opts}]
  (when (nil? (:schema-registry opts))
    (throw (IllegalArgumentException. "Missing required :schema-registry in make-bus options.")))
  (let [base-opts (merge {:mode mode :max-depth max-depth} opts)
        final-opts (if (:tx-store base-opts)
                     (merge {:tx-retention-ms default-tx-retention-ms
                             :tx-cleanup-interval-ms default-tx-cleanup-interval-ms}
                            base-opts)
                     base-opts)
        bus-map {:listeners (atom {})
                 :closed?   (atom false)
                 :opts      final-opts}
        bus-with-executor
        (case mode
          :unlimited
          (assoc bus-map :executor (Executors/newVirtualThreadPerTaskExecutor))

          :buffered
          (let [{:keys [buffer-size]
                 :or   {buffer-size 1024}} opts
                queue (ArrayBlockingQueue. buffer-size)
                executor (Executors/newVirtualThreadPerTaskExecutor)]
            (assoc bus-map
                   :executor executor
                   :queue queue
                   :consumers (atom []))))]
    (doto (cond-> bus-with-executor
            (:tx-store final-opts)
            (assoc :tx-store (tx-store/init-store (:tx-store final-opts))
                   :tx-results (atom {})
                   :tx-worker (atom nil)
                   :tx-stop (atom false)
                   :tx-cleanup-last (atom (System/currentTimeMillis))
                   :tx-executor (Executors/newVirtualThreadPerTaskExecutor)))
      ((fn [bus]
         (when (= :buffered mode)
           (start-buffered-consumers! bus))))
      ((fn [bus]
         (when (:tx-store bus)
           (start-tx-worker! bus)))))))

(defn- ensure-not-closed! [bus]
  (when @(:closed? bus)
    (throw (IllegalStateException. "Event bus is closed."))))

;; ============================
;; Public API
;; ============================

(defn subscribe
  "Subscribes a handler to an event type.
   The handler function must have the signature `(fn [bus envelope])`."
  [bus event-type handler & {:keys [schema meta]}]
  (ensure-not-closed! bus)
  (let [handler-id (UUID/randomUUID)]
    (swap! (:listeners bus) update event-type conj
           (cond-> {:handler handler
                    :id handler-id}
             schema (assoc :schema schema)
             meta (assoc :meta meta))))
  bus)

(defn publish
  "Publishes an event.
   To derive an event from another, pass the parent as `:parent-envelope` in opts.
   Returns the created event envelope."
  [bus event-type payload & [opts]]
  (ensure-not-closed! bus)
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
        publish-schema (get event-schemas schema-version)
        handlers (current-handlers bus event-type)]
    (when-not publish-schema
      (log! bus :error {:event :publish-schema-missing
                        :event-type event-type
                        :schema-version schema-version
                        :correlation-id (:correlation-id envelope)})
      (throw (IllegalStateException.
              (str "Missing schema for event " event-type " version " schema-version))))
    (when-not (m/validate publish-schema payload)
      (log! bus :error {:event :publish-schema-validation-failed
                        :event-type event-type
                        :schema-version schema-version
                        :correlation-id (:correlation-id envelope)
                        :payload payload
                        :errors (me/humanize (m/explain publish-schema payload))})
      (throw (IllegalStateException.
              (str "Publish schema validation failed for event " event-type " version " schema-version))))
    (case (:mode (:opts bus))
      :unlimited (dispatch-event-now! bus envelope handlers)
      :buffered (enqueue-buffered-event! bus envelope))
    (log! bus :info {:event :event-published
                     :correlation-id (:correlation-id envelope)
                     :envelope envelope})
    envelope))

(defn transact
  "Atomically records events in internal DB and processes them via handlers.
   Returns a map with :op-id, :result-promise, and :result-chan."
  [bus events]
  (ensure-not-closed! bus)
  (ensure-tx-store! bus)
  (when-not (seq events)
    (throw (IllegalArgumentException. "transact requires a non-empty list of events.")))
  (let [tx-id (UUID/randomUUID)
        now (now-inst)
        registry (-> bus :opts :schema-registry)
        listeners @(:listeners bus)
        result-promise (promise)
        result-chan (async/promise-chan)
        result-mult (async/mult result-chan)]
    (swap! (:tx-results bus) assoc tx-id {:promise result-promise
                                          :chan result-chan
                                          :mult result-mult})
    (try
      (doseq [{:keys [event-type payload module schema-version]} events]
        (ensure-module! module)
        (when-not event-type
          (throw (IllegalArgumentException. "Missing :event-type in transact event.")))
        (let [schema-version (or schema-version "1.0")
              event-schemas (get registry event-type)
              publish-schema (get event-schemas schema-version)]
          (when-not publish-schema
            (log! bus :error {:event :publish-schema-missing
                              :event-type event-type
                              :schema-version schema-version
                              :correlation-id tx-id})
            (throw (IllegalStateException.
                    (str "Missing schema for event " event-type " version " schema-version))))
          (when-not (m/validate publish-schema payload)
            (log! bus :error {:event :publish-schema-validation-failed
                              :event-type event-type
                              :schema-version schema-version
                              :correlation-id tx-id
                              :payload payload
                              :errors (me/humanize (m/explain publish-schema payload))})
            (throw (IllegalStateException.
                    (str "Publish schema validation failed for event " event-type " version " schema-version))))))
      (let [{:keys [tx-data handler-count]}
            (tx-store/build-tx-data (:tx-store bus) tx-id now events listeners)]
        (tx-store/transact! (:tx-store bus) tx-data)
        (log! bus :info {:event :tx-created
                         :tx-id tx-id})
        (when (zero? handler-count)
          (complete-tx! bus tx-id true nil))
        {:op-id tx-id
         :result-promise result-promise
         :result-chan result-chan
         :result-mult result-mult})
      (catch Throwable e
        (swap! (:tx-results bus) dissoc tx-id)
        (throw e)))))

(defn unsubscribe
  "Unsubscribes a handler by its reference or metadata."
  [bus event-type matcher]
  (ensure-not-closed! bus)
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
     (log! bus :info {:event :bus-closing})
     (when-let [tx-stop (:tx-stop bus)]
       (reset! tx-stop true))
     (when-let [tx-store (:tx-store bus)]
       (when-let [writer (:writer tx-store)]
         (try
           (.close ^java.io.Writer writer)
           (catch Throwable _ nil)))
       (when-let [flusher-stop (:flusher-stop tx-store)]
         (reset! flusher-stop true))
       (when-let [scheduler (:scheduler tx-store)]
         (try
           (.shutdown ^java.util.concurrent.ExecutorService scheduler)
           (catch Throwable _ nil))))
     (when-let [tx-executor ^ExecutorService (:tx-executor bus)]
       (.shutdown tx-executor))
     (let [executor ^ExecutorService (:executor bus)]
       (.shutdown executor)
       (when-let [consumers (:consumers bus)]
         (run! #(.cancel % true) @consumers))
       (try
         (when-not (.awaitTermination executor timeout TimeUnit/MILLISECONDS)
           (log! bus :warn {:event :shutdown-timeout}))
         (catch InterruptedException _
           (.interrupt (Thread/currentThread))))
       (log! bus :info {:event :bus-closed})))))

;; ============================
;; Utility Functions
;; ============================

(defn listener-count
  "Returns the number of listeners for an event type or for the entire bus."
  ([bus]
   (apply + (map count (vals @(:listeners bus)))))
  ([bus event-type]
   (count (get @(:listeners bus) event-type []))))
