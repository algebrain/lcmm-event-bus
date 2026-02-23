(ns support
  (:require [clojure.test :refer [is]]
            [event-bus :as bus]
            [malli.core :as m])
  (:import [java.io File]
           [java.sql DriverManager]
           [java.nio.file Files]
           [java.util UUID]
           [java.util.concurrent CountDownLatch TimeUnit]))

(defn await-on-latch [^CountDownLatch latch]
  (.await latch 1 TimeUnit/SECONDS))

(def any-schema (m/schema :any))

(def test-registry
  {:test/event {"1.0" any-schema}
   :multi/event {"1.0" any-schema}
   :event/inc {"1.0" any-schema}
   :event/child {"1.0" any-schema}
   :event/parent {"1.0" any-schema}
   :event/a {"1.0" any-schema}
   :event/b {"1.0" any-schema}
   :event-1 {"1.0" any-schema}
   :event-2 {"1.0" any-schema}
   :event-3 {"1.0" any-schema}
   :event-4 {"1.0" any-schema}
   :log/break {"1.0" any-schema}
   :log/event {"1.0" (m/schema [:map [:x :int]])}
   :schema/event {"1.0" (m/schema [:map [:x :int]])}
   :event {"1.0" any-schema}
   :partial/event {"1.0" any-schema}
   :c1 {"1.0" any-schema}
   :c2 {"1.0" any-schema}
   :c3 {"1.0" any-schema}
   :any/event {"1.0" any-schema}
   :versioned/event {"1.0" (m/schema [:map [:v :int]])
                     "2.0" (m/schema [:map [:v :string]])}})

(defn make-test-bus [& {:as opts}]
  (apply bus/make-bus (apply concat (merge {:schema-registry test-registry} opts))))

(defn- make-sqlite-config []
  (let [^File file (File/createTempFile "event-bus-" ".db")]
    (.deleteOnExit file)
    {:jdbc-url (str "jdbc:sqlite:" (.getAbsolutePath file))}))

(defn make-tx-store
  ([] (make-tx-store :datahike))
  ([backend]
   (case backend
     :datahike {:db/type :datahike
                :datahike/config {:store {:backend :mem
                                          :id (str (UUID/randomUUID))}
                                  :schema-flexibility :write}}
     :sqlite {:db/type :sqlite
              :sqlite/config (make-sqlite-config)}
     (throw (IllegalArgumentException.
             (str "Unsupported backend in tests: " backend))))))

(defn run-with-timeout!
  [timeout-ms f]
  (let [result (deref (future (f)) timeout-ms ::timeout)]
    (when (= result ::timeout)
      (is false (str "Test timed out after " timeout-ms " ms")))))

(defn wait-until
  [timeout-ms pred]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (cond
        (pred) true
        (< (System/currentTimeMillis) deadline) (do (Thread/sleep 20) (recur))
        :else false))))

(defn sqlite-count
  [jdbc-url table-name]
  (with-open [conn (DriverManager/getConnection jdbc-url)
              stmt (.prepareStatement conn (str "SELECT COUNT(*) FROM " table-name))
              rs (.executeQuery stmt)]
    (if (.next rs) (.getLong rs 1) 0)))

(defn sqlite-logger []
  (fn [_level data]
    (when (= :tx-worker-failed (:event data))
      (println "SQLite tx-worker failed:" data))))

(defn temp-dir []
  (.toFile (Files/createTempDirectory "event-bus-dump-"
                                      (make-array java.nio.file.attribute.FileAttribute 0))))
