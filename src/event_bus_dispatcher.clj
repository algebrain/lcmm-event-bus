(ns event-bus-dispatcher
  (:require [event-bus-persistence :as p]
            [event-bus-runtime :as runtime]))

(defn dispatch-once!
  [bus store {:keys [dispatcher-batch-size dispatcher-max-attempts]
              :or {dispatcher-batch-size 100
                   dispatcher-max-attempts 5}}]
  (let [batch (p/fetch-pending! store dispatcher-batch-size)]
    (doseq [{:keys [message-id envelope attempts]} batch]
      (try
        (runtime/deliver! bus envelope)
        (p/mark-sent! store message-id)
        (runtime/log! bus :info {:event :event-dispatched
                                 :message-id message-id
                                 :message-type (:message-type envelope)})
        (catch Throwable e
          (let [new-attempts (p/record-attempt! store message-id)]
            (runtime/log! bus :error {:event :event-dispatch-failed
                                      :message-id message-id
                                      :message-type (:message-type envelope)
                                      :attempts new-attempts
                                      :exception e})
            (when (>= new-attempts dispatcher-max-attempts)
              (p/mark-failed! store message-id)
              (runtime/log! bus :error {:event :event-dispatch-give-up
                                        :message-id message-id
                                        :message-type (:message-type envelope)
                                        :attempts new-attempts}))))))))

(defn start-dispatcher
  [bus store {:keys [dispatcher-enabled dispatcher-interval-ms]
              :or {dispatcher-enabled true
                   dispatcher-interval-ms 500}}]
  (when dispatcher-enabled
    (let [stop? (atom false)
          fut (future
                (while (not @stop?)
                  (try
                    (dispatch-once! bus store (:opts bus))
                    (catch Throwable e
                      (runtime/log! bus :error {:event :dispatcher-crash
                                                :exception e})))
                  (Thread/sleep dispatcher-interval-ms)))]
      {:stop? stop?
       :future fut})))

(defn stop-dispatcher [{:keys [stop? future]}]
  (when stop?
    (reset! stop? true))
  (when future
    @future)
  true)
