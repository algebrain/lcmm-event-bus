(ns perf
  (:require [clojure.string :as str]
            [event-bus :as bus]
            [malli.core :as m]
            [taoensso.timbre :as timbre])
  (:import [java.io File]
           [java.util UUID]
           [java.util.concurrent CountDownLatch TimeUnit]))

(def any-schema (m/schema :any))

(def registry
  {:bench/event {"1.0" any-schema}
   :bench/tx {"1.0" any-schema}})

(defn- parse-long* [s]
  (try
    (Long/parseLong s)
    (catch Throwable _ nil)))

(defn- parse-args [args]
  (let [pairs (keep (fn [arg]
                      (when (str/starts-with? arg "--")
                        (let [[k v] (str/split (subs arg 2) #"=" 2)]
                          (when (and k v)
                            [(keyword k) v]))))
                    args)
        m (into {} pairs)
        mode (some-> (get m :mode) keyword)
        backend (some-> (get m :backend) keyword)]
    {:mode (or mode :unlimited)
     :buffer-size (or (parse-long* (get m :buffer-size)) 1024)
     :concurrency (or (parse-long* (get m :concurrency)) 4)
     :subscribers (or (parse-long* (get m :subscribers)) 1)
     :events (or (parse-long* (get m :events)) 10000)
     :latency-samples (or (parse-long* (get m :latency-samples)) 1000)
     :tx-count (or (parse-long* (get m :tx-count)) 200)
     :tx-batch (or (parse-long* (get m :tx-batch)) 1)
     :backend (or backend :sqlite)
     :handler-backoff-ms (or (parse-long* (get m :handler-backoff-ms)) 10)
     :handler-max-retries (or (parse-long* (get m :handler-max-retries)) 2)
     :tx-timeout-ms (or (parse-long* (get m :tx-timeout-ms)) 2000)}))

(defn- now-ns [] (System/nanoTime))

(defn- nanos->ms [n]
  (/ (double n) 1000000.0))

(defn- percentile [sorted-values p]
  (let [n (count sorted-values)]
    (if (zero? n)
      0.0
      (let [idx (long (Math/floor (* (dec n) p)))]
        (double (nth sorted-values idx))))))

(defn- stats [values]
  (let [sorted (vec (sort values))]
    {:count (count sorted)
     :p50 (percentile sorted 0.50)
     :p95 (percentile sorted 0.95)
     :p99 (percentile sorted 0.99)}))

(defn- make-sqlite-config []
  (let [^File file (File/createTempFile "event-bus-bench-" ".db")]
    (.deleteOnExit file)
    {:jdbc-url (str "jdbc:sqlite:" (.getAbsolutePath file))}))

(defn- make-tx-store [backend]
  (case backend
    :datahike {:db/type :datahike
               :datahike/config {:store {:backend :mem
                                         :id (str (UUID/randomUUID))}
                                 :schema-flexibility :write}}
    :sqlite {:db/type :sqlite
             :sqlite/config (make-sqlite-config)}
    (throw (IllegalArgumentException.
            (str "Unsupported backend: " backend)))))

(defn- make-bench-bus [{:keys [mode buffer-size concurrency] :as opts}]
  (let [mode (or mode :unlimited)
        buffer-size (or buffer-size 1024)
        concurrency (or concurrency 4)]
    (apply bus/make-bus
           (apply concat
                  (merge {:schema-registry registry
                          :mode mode
                          :buffer-size buffer-size
                          :concurrency concurrency}
                         opts)))))

(defn- await-latch [^CountDownLatch latch timeout-ms]
  (.await latch timeout-ms TimeUnit/MILLISECONDS))

(defn- publish-throughput [{:keys [events subscribers] :as opts}]
  (let [bus (make-bench-bus opts)
        latch (CountDownLatch. (* events subscribers))]
    (dotimes [_ subscribers]
      (bus/subscribe bus :bench/event
                     (fn [_ _]
                       (.countDown latch))))
    (let [start (now-ns)]
      (dotimes [i events]
        (bus/publish bus :bench/event {:n i} {:module :bench}))
      (await-latch latch 10000)
      (let [elapsed (nanos->ms (- (now-ns) start))
            eps (/ events (/ elapsed 1000.0))]
        (bus/close bus)
        {:elapsed-ms elapsed
         :events-per-sec eps}))))

(defn- publish-latency [{:keys [events] :as opts}]
  (let [bus (make-bench-bus opts)
        latencies (atom [])
        latch (CountDownLatch. events)]
    (bus/subscribe bus :bench/event
                   (fn [_ envelope]
                     (let [start-ns (get-in envelope [:payload :start-ns])
                           delta (nanos->ms (- (now-ns) start-ns))]
                       (swap! latencies conj delta)
                       (.countDown latch))))
    (dotimes [_ events]
      (bus/publish bus :bench/event {:start-ns (now-ns)} {:module :bench}))
    (await-latch latch 10000)
    (let [s (stats @latencies)]
      (bus/close bus)
      s)))

(defn- buffered-backpressure [{:keys [events] :as opts}]
  (let [bus (make-bench-bus opts)
        failures (atom 0)]
    (bus/subscribe bus :bench/event (fn [_ _] (Thread/sleep 5)))
    (dotimes [i events]
      (try
        (bus/publish bus :bench/event {:n i} {:module :bench})
        (catch IllegalStateException _
          (swap! failures inc))))
    (bus/close bus)
    {:attempts events
     :failures @failures
     :failure-rate (if (zero? events) 0.0 (/ @failures (double events)))}))

(defn- transact-throughput [{:keys [tx-count tx-batch backend handler-backoff-ms handler-max-retries tx-timeout-ms]}]
  (let [bus (make-bench-bus {:tx-store (make-tx-store backend)
                             :handler-backoff-ms handler-backoff-ms
                             :handler-max-retries handler-max-retries})]
    (bus/subscribe bus :bench/tx (fn [_ _] true))
    (let [start (now-ns)
          timeouts (atom 0)]
      (dotimes [_ tx-count]
        (let [events (vec (repeatedly tx-batch
                                      (fn [] {:event-type :bench/tx
                                              :payload {:ok true}
                                              :module :bench})))
              tx-result (deref (future (bus/transact bus events))
                               tx-timeout-ms
                               ::timeout)]
          (if (= tx-result ::timeout)
            (swap! timeouts inc)
            (let [{:keys [result-promise]} tx-result]
              (when (= ::timeout (deref result-promise tx-timeout-ms ::timeout))
                (swap! timeouts inc))))))
      (let [elapsed (nanos->ms (- (now-ns) start))
            tps (/ tx-count (/ elapsed 1000.0))
            eps (/ (* tx-count tx-batch) (/ elapsed 1000.0))]
        (bus/close bus)
        {:elapsed-ms elapsed
         :tx-per-sec tps
         :events-per-sec eps
         :timeouts @timeouts}))))

(defn- transact-latency [{:keys [latency-samples tx-batch backend handler-backoff-ms handler-max-retries tx-timeout-ms]}]
  (let [bus (make-bench-bus {:tx-store (make-tx-store backend)
                             :handler-backoff-ms handler-backoff-ms
                             :handler-max-retries handler-max-retries})]
    (bus/subscribe bus :bench/tx (fn [_ _] true))
    (let [latencies (atom [])
          timeouts (atom 0)]
      (dotimes [_ latency-samples]
        (let [events (vec (repeatedly tx-batch
                                      (fn [] {:event-type :bench/tx
                                              :payload {:ok true}
                                              :module :bench})))
              start (now-ns)
              tx-result (deref (future (bus/transact bus events))
                               tx-timeout-ms
                               ::timeout)]
          (if (= tx-result ::timeout)
            (swap! timeouts inc)
            (let [{:keys [result-promise]} tx-result
                  result (deref result-promise tx-timeout-ms ::timeout)]
              (if (= result ::timeout)
                (swap! timeouts inc)
                (let [elapsed (nanos->ms (- (now-ns) start))]
                  (swap! latencies conj elapsed)))))))
      (let [s (stats @latencies)]
        (bus/close bus)
        (assoc s :timeouts @timeouts)))))

(defn -main [& args]
  (let [{:keys [mode buffer-size concurrency subscribers events latency-samples
                tx-count tx-batch backend handler-backoff-ms handler-max-retries tx-timeout-ms] :as opts}
        (parse-args args)]
    (when (= backend :datahike)
      (timbre/merge-config! {:min-level :warn}))
    (println "BENCH OPTIONS" opts)

    (println "
PUBLISH THROUGHPUT")
    (println (publish-throughput {:mode mode
                                  :buffer-size buffer-size
                                  :concurrency concurrency
                                  :subscribers subscribers
                                  :events events}))

    (println "
PUBLISH LATENCY")
    (println (publish-latency {:mode mode
                               :buffer-size buffer-size
                               :concurrency concurrency
                               :events latency-samples}))

    (when (= mode :buffered)
      (println "
BUFFERED BACKPRESSURE")
      (println (buffered-backpressure {:mode mode
                                       :buffer-size buffer-size
                                       :concurrency concurrency
                                       :events events})))

    (println "
TRANSACT THROUGHPUT")
    (println (transact-throughput {:tx-count tx-count
                                   :tx-batch tx-batch
                                   :backend backend
                                   :handler-backoff-ms handler-backoff-ms
                                   :handler-max-retries handler-max-retries
                                   :tx-timeout-ms tx-timeout-ms}))

    (println "
TRANSACT LATENCY")
    (println (transact-latency {:latency-samples latency-samples
                                :tx-batch tx-batch
                                :backend backend
                                :handler-backoff-ms handler-backoff-ms
                                :handler-max-retries handler-max-retries
                                :tx-timeout-ms tx-timeout-ms}))))
