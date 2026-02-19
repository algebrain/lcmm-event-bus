(ns event-bus-runtime
  (:require [event-bus-persistence :as persistence]
            [malli.core :as m]
            [malli.error :as me])
  (:import [java.util.concurrent Executors ExecutorService TimeUnit ArrayBlockingQueue]))

(defn log!
  "Calls the configured logger, if any."
  [bus level data]
  (when-let [logger (-> bus :opts :logger)]
    (try
      (logger level data)
      (catch Throwable _
        ;; Never allow logger failures to break the critical path
        nil))))

(defn ensure-not-closed! [bus]
  (when @(:closed? bus)
    (throw (IllegalStateException. "Event bus is closed."))))

(defn submit-task [bus f]
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

(defn deliver!
  "Delivers an already-validated envelope to subscribers."
  [bus envelope]
  (ensure-not-closed! bus)
  (let [event-type (:message-type envelope)
        handlers (get @(:listeners bus) event-type [])
        store (:persistence bus)]
    (doseq [{:keys [handler schema inbox]} handlers]
      (let [deliver? (if inbox
                       (let [msg-id (:message-id envelope)]
                         (when-not (persistence/was-received? store msg-id)
                           (persistence/mark-received! store msg-id)
                           true))
                       true)]
        (when deliver?
          (if-not schema
            (submit-task bus #(handler bus envelope))
            (if (m/validate schema (:payload envelope))
              (submit-task bus #(handler bus envelope))
              (log! bus :warn {:event :schema-validation-failed
                               :event-type event-type
                               :correlation-id (:correlation-id envelope)
                               :payload (:payload envelope)
                               :errors (me/humanize (m/explain schema (:payload envelope)))}))))))
    true))
