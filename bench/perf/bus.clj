(ns perf.bus
  (:require [event-bus :as bus]
            [malli.core :as m])
  (:import [java.io File]
           [java.util UUID]))

(def any-schema (m/schema :any))

(def registry
  {:bench/event {"1.0" any-schema}
   :bench/tx {"1.0" any-schema}})

(defn make-sqlite-config []
  (let [^File file (File/createTempFile "event-bus-bench-" ".db")]
    (.deleteOnExit file)
    {:jdbc-url (str "jdbc:sqlite:" (.getAbsolutePath file))}))

(defn make-tx-store [backend opts]
  (case backend
    :datahike {:db/type :datahike
               :datahike/config {:store {:backend :mem
                                         :id (str (UUID/randomUUID))}
                                 :schema-flexibility :write}}
    :filelog {:db/type :filelog
              :filelog/config (cond-> {:path (str (File/createTempFile "event-bus-filelog-" ".log"))}
                                (:fsync-interval-ms opts) (assoc :fsync-interval-ms (:fsync-interval-ms opts)))}
    :sqlite {:db/type :sqlite
             :sqlite/config (make-sqlite-config)}
    (throw (IllegalArgumentException.
            (str "Unsupported backend: " backend)))))

(defn bench-bus-opts [{:keys [mode buffer-size concurrency] :as opts}]
  (merge {:schema-registry registry
          :mode (or mode :unlimited)
          :buffer-size (or buffer-size 1024)
          :concurrency (or concurrency 4)}
         (select-keys opts [:logger
                            :log-payload
                            :log-payload-max-chars
                            :tx-store
                            :tx-handler-timeout
                            :handler-max-retries
                            :handler-backoff-ms
                            :tx-retention-ms
                            :tx-cleanup-interval-ms])))

(defn make-bench-bus [opts]
  (apply bus/make-bus (apply concat (bench-bus-opts opts))))
