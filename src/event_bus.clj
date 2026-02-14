(ns event-bus
  (:require [malli.core :as m]
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
    (logger level data)))

(defn- make-envelope
  "Creates a root envelope for a new event chain."
  [event-type payload {:keys [correlation-id schema-version]
                       :or   {schema-version "1.0"}}]
  {:message-id      (UUID/randomUUID)
   :correlation-id  (or correlation-id (UUID/randomUUID))
   :causation-path  #{}
   :message-type    event-type
   :schema-version  schema-version
   :payload         payload})

(defn- derive-envelope
  "Creates a child envelope based on a parent, enabling causality tracking."
  [parent-envelope new-event-type new-payload {:keys [max-depth] :as _bus-opts}]
  (let [new-causation-path (conj (:causation-path parent-envelope)
                                 (:message-type parent-envelope))]
    (when (contains? new-causation-path new-event-type)
      (throw (IllegalStateException.
               (str "Cycle detected: event " new-event-type " already in causation path."))))
    (when (and max-depth (> (count new-causation-path) max-depth))
      (throw (IllegalStateException.
               (str "Max depth exceeded: " (count new-causation-path) " > " max-depth))))
    (assoc (make-envelope new-event-type new-payload
                          {:correlation-id (:correlation-id parent-envelope)})
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
  [& {:keys [mode max-depth]
      :or   {mode :unlimited, max-depth 20}
      :as   opts}]
  (let [bus-map {:listeners (atom {})
                 :closed?   (atom false)
                 :opts      (merge {:mode mode :max-depth max-depth} opts)}]
    (case mode
      :unlimited
      (assoc bus-map :executor (Executors/newVirtualThreadPerTaskExecutor))

      :buffered
      (let [{:keys [buffer-size concurrency]
             :or   {buffer-size 1024, concurrency 4}} opts
            queue (ArrayBlockingQueue. buffer-size)
            executor (Executors/newVirtualThreadPerTaskExecutor)
            consumers (doall
                        (repeatedly concurrency
                                    #(future
                                       (while (not (.isShutdown executor))
                                         (try
                                           (let [task (.take queue)]
                                             (when task (task)))
                                           (catch InterruptedException _
                                             ;; Allow thread to exit
                                             ))))))]
        (assoc bus-map
               :executor executor
               :queue queue
               :consumers consumers)))))

;; ============================
;; Internal Task Submission
;; ============================

(defn- ensure-not-closed! [bus]
  (when @(:closed? bus)
    (throw (IllegalStateException. "Event bus is closed."))))

(defn- submit-task [bus f]
  (ensure-not-closed! bus)
  (let [task (fn []
               (try
                 (f)
                 (catch Throwable e
                   (log! bus :error {:event :handler-failed
                                     :exception e}))))]
    (case (:mode (:opts bus))
      :unlimited
      (.submit ^ExecutorService (:executor bus) ^Runnable task)

      :buffered
      (let [^ArrayBlockingQueue queue (:queue bus)]
        (when-not (.offer queue task)
          (log! bus :error {:event :buffer-full})
          (throw (IllegalStateException. "Event bus buffer is full.")))))))

;; ============================
;; Public API
;; ============================

(defn subscribe
  "Subscribes a handler to an event type.
   The handler function must have the signature `(fn [bus envelope])`."
  [bus event-type handler & {:keys [schema meta]}]
  (ensure-not-closed! bus)
  (swap! (:listeners bus) update event-type conj
         (cond-> {:handler handler}
           schema (assoc :schema schema)
           meta (assoc :meta meta)))
  bus)

(defn publish
  "Publishes an event.
   To derive an event from another, pass the parent as `:parent-envelope` in opts.
   Returns the number of handlers invoked."
  [bus event-type payload & [opts]]
  (ensure-not-closed! bus)
  (let [parent-envelope (:parent-envelope opts)
        envelope (if parent-envelope
                   (derive-envelope parent-envelope event-type payload (:opts bus))
                   (make-envelope event-type payload (:opts bus)))
        handlers (get @(:listeners bus) event-type [])]
    (log! bus :info {:event :event-published
                     :envelope envelope})
    (doseq [{:keys [handler schema]} handlers]
      (if-not schema
        (submit-task bus #(handler bus envelope))
        (if (m/validate schema (:payload envelope))
          (submit-task bus #(handler bus envelope))
          (log! bus :warn {:event :schema-validation-failed
                           :event-type event-type
                           :correlation-id (:correlation-id envelope)
                           :payload (:payload envelope)
                           :errors (me/humanize (m/explain schema (:payload envelope)))}))))
    (count handlers)))

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
     (let [executor ^ExecutorService (:executor bus)]
       (.shutdown executor)
       (when-let [consumers (:consumers bus)]
         (run! future-cancel consumers))
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
