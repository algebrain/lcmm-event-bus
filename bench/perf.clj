(ns perf
  (:require [clojure.string :as str]
            [event-bus :as bus]
            [malli.core :as m]
            [taoensso.timbre :as timbre])
  (:import [java.io File]
           [java.lang.management ManagementFactory]
           [java.util UUID]
           [java.util.concurrent CountDownLatch TimeUnit]))

(def any-schema (m/schema :any))

(def registry
  {:bench/event {"1.0" any-schema}
   :bench/tx {"1.0" any-schema}})

(def ^:private default-opts
  {:mode :unlimited
   :buffer-size 1024
   :concurrency 4
   :subscribers 1
   :events 10000
   :latency-samples 1000
   :tx-count 200
   :tx-batch 1
   :backend :sqlite
   :handler-backoff-ms 10
   :handler-max-retries 2
   :tx-timeout-ms 2000
   :payload-bytes 128
   :warmup-iterations 1
   :measure-iterations 3
   :duration-ms 3000
   :drain-timeout-ms 5000
   :only :all
   :quick? false
   :scenario nil
   :fsync-interval-ms nil})

(def ^:private quick-defaults
  {:events 1000
   :latency-samples 250
   :tx-count 30
   :payload-bytes 64
   :warmup-iterations 1
   :measure-iterations 1
   :duration-ms 1000
   :drain-timeout-ms 1500})

(def ^:private known-groups
  #{:all :publish :buffered :memory :transact})

(defn- parse-long* [s]
  (try
    (Long/parseLong s)
    (catch Throwable _ nil)))

(defn- parse-flagged-args [args]
  (reduce
   (fn [{:keys [flags kv]} arg]
     (cond
       (= arg "--quick")
       {:flags (conj flags :quick) :kv kv}

       (str/starts-with? arg "--")
       (let [[k v] (str/split (subs arg 2) #"=" 2)]
         (if v
           {:flags flags :kv (assoc kv (keyword k) v)}
           {:flags (conj flags (keyword k)) :kv kv}))

       :else
       {:flags flags :kv kv}))
   {:flags #{} :kv {}}
   args))

(defn- build-payload [payload-bytes seed]
  (let [base (str "payload-" seed "-")
        needed (max 1 (long payload-bytes))
        repeated (apply str (take (inc (quot needed (count base))) (repeat base)))]
    {:n seed
     :blob (subs repeated 0 needed)}))

(defn- parse-args [args]
  (let [{:keys [flags kv]} (parse-flagged-args args)
        quick? (contains? flags :quick)
        explicit-keys (set (keys kv))
        mode (some-> (get kv :mode) keyword)
        backend (some-> (get kv :backend) keyword)
        only (some-> (get kv :only) keyword)
        scenario (some-> (get kv :scenario) keyword)
        numeric-values {:buffer-size (parse-long* (get kv :buffer-size))
                        :concurrency (parse-long* (get kv :concurrency))
                        :subscribers (parse-long* (get kv :subscribers))
                        :events (parse-long* (get kv :events))
                        :latency-samples (parse-long* (get kv :latency-samples))
                        :tx-count (parse-long* (get kv :tx-count))
                        :tx-batch (parse-long* (get kv :tx-batch))
                        :handler-backoff-ms (parse-long* (get kv :handler-backoff-ms))
                        :handler-max-retries (parse-long* (get kv :handler-max-retries))
                        :tx-timeout-ms (parse-long* (get kv :tx-timeout-ms))
                        :payload-bytes (parse-long* (get kv :payload-bytes))
                        :warmup-iterations (parse-long* (get kv :warmup-iterations))
                        :measure-iterations (parse-long* (get kv :measure-iterations))
                        :duration-ms (parse-long* (get kv :duration-ms))
                        :drain-timeout-ms (parse-long* (get kv :drain-timeout-ms))
                        :fsync-interval-ms (parse-long* (get kv :fsync-interval-ms))}
        merged (merge default-opts
                      (when quick? quick-defaults)
                      (into {}
                            (keep (fn [[k v]]
                                    (when (some? v)
                                      [k v])))
                            numeric-values))
        merged (cond-> merged
                 mode (assoc :mode mode)
                 backend (assoc :backend backend)
                 only (assoc :only only)
                 scenario (assoc :scenario scenario)
                 quick? (assoc :quick? true))
        merged (if (contains? explicit-keys :only)
                 merged
                 (assoc merged :only :all))]
    (when-not (contains? known-groups (:only merged))
      (throw (IllegalArgumentException.
              (str "Unsupported --only value: " (:only merged)))))
    merged))

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
     :min (if (seq sorted) (double (first sorted)) 0.0)
     :p50 (percentile sorted 0.50)
     :p95 (percentile sorted 0.95)
     :p99 (percentile sorted 0.99)
     :max (if (seq sorted) (double (peek sorted)) 0.0)
     :avg (if (seq sorted)
            (/ (reduce + 0.0 sorted) (double (count sorted)))
            0.0)}))

(defn- average [values]
  (if (seq values)
    (/ (reduce + 0.0 values) (double (count values)))
    0.0))

(defn- aggregate-runs [runs]
  (reduce
   (fn [acc run]
     (reduce-kv
      (fn [m k v]
        (if (number? v)
          (update m k (fnil conj []) (double v))
          (assoc m k v)))
      acc
      run))
   {}
   runs))

(defn- summarize-runs [runs]
  (let [aggregated (aggregate-runs runs)]
    (into {:runs (count runs)}
          (map (fn [[k v]]
                 (if (vector? v)
                   [k (average v)]
                   [k v])))
          aggregated)))

(defn- gc-stats []
  (reduce
   (fn [acc bean]
     (-> acc
         (update :gc-count + (max 0 (.getCollectionCount bean)))
         (update :gc-time-ms + (max 0 (.getCollectionTime bean)))))
   {:gc-count 0 :gc-time-ms 0}
   (ManagementFactory/getGarbageCollectorMXBeans)))

(defn- used-heap-bytes []
  (let [rt (Runtime/getRuntime)]
    (- (.totalMemory rt) (.freeMemory rt))))

(defn- settle-memory! []
  (dotimes [_ 3]
    (System/gc)
    (Thread/sleep 50)))

(defn- capture-memory []
  (merge {:used-heap-bytes (used-heap-bytes)}
         (gc-stats)))

(defn- bytes->kb [n]
  (/ (double n) 1024.0))

(defn- memory-summary [before after]
  (let [delta-bytes (- (:used-heap-bytes after) (:used-heap-bytes before))]
    {:used-heap-before-kb (bytes->kb (:used-heap-bytes before))
     :used-heap-after-kb (bytes->kb (:used-heap-bytes after))
     :heap-delta-kb (bytes->kb delta-bytes)
     :gc-count-delta (- (:gc-count after) (:gc-count before))
     :gc-time-delta-ms (- (:gc-time-ms after) (:gc-time-ms before))}))

(defn- make-sqlite-config []
  (let [^File file (File/createTempFile "event-bus-bench-" ".db")]
    (.deleteOnExit file)
    {:jdbc-url (str "jdbc:sqlite:" (.getAbsolutePath file))}))

(defn- make-tx-store [backend opts]
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

(defn- bench-bus-opts [{:keys [mode buffer-size concurrency] :as opts}]
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

(defn- make-bench-bus [opts]
  (apply bus/make-bus (apply concat (bench-bus-opts opts))))

(defn- await-latch [^CountDownLatch latch timeout-ms]
  (.await latch timeout-ms TimeUnit/MILLISECONDS))

(defn- run-scenario* [{:keys [name warmup-iterations measure-iterations scenario-fn] :as opts}]
  (dotimes [_ warmup-iterations]
    (scenario-fn opts))
  (let [runs (vec (repeatedly measure-iterations #(scenario-fn opts)))]
    {:scenario name
     :summary (summarize-runs runs)
     :runs runs}))

(defn- publish-call-latency-run [{:keys [events payload-bytes] :as opts}]
  (let [bus (make-bench-bus opts)
        durations (double-array events)]
    (try
      (dotimes [i events]
        (let [payload (build-payload payload-bytes i)
              start (now-ns)]
          (bus/publish bus :bench/event payload {:module :bench})
          (aset-double durations i (nanos->ms (- (now-ns) start)))))
      (stats (vec durations))
      (finally
        (bus/close bus)))))

(defn- publish-end-to-end-latency-run [{:keys [events payload-bytes drain-timeout-ms] :as opts}]
  (let [bus (make-bench-bus opts)
        latch (CountDownLatch. events)
        durations (double-array events)]
    (try
      (bus/subscribe bus :bench/event
                     (fn [_ envelope]
                       (let [idx (int (get-in envelope [:payload :idx]))
                             delta (nanos->ms (- (now-ns) (long (get-in envelope [:payload :start-ns]))))]
                         (aset-double durations idx delta)
                         (.countDown latch))))
      (dotimes [i events]
        (bus/publish bus
                     :bench/event
                     (assoc (build-payload payload-bytes i)
                            :idx i
                            :start-ns (now-ns))
                     {:module :bench}))
      (let [completed? (await-latch latch drain-timeout-ms)]
        (assoc (stats (vec durations))
               :completed? (if completed? 1.0 0.0)))
      (finally
        (bus/close bus)))))

(defn- publish-throughput-run [{:keys [events subscribers payload-bytes drain-timeout-ms] :as opts}]
  (let [bus (make-bench-bus opts)
        latch (CountDownLatch. (* events subscribers))]
    (try
      (dotimes [_ subscribers]
        (bus/subscribe bus :bench/event (fn [_ _] (.countDown latch))))
      (let [start (now-ns)]
        (dotimes [i events]
          (bus/publish bus :bench/event (build-payload payload-bytes i) {:module :bench}))
        (let [completed? (await-latch latch drain-timeout-ms)
              elapsed (nanos->ms (- (now-ns) start))
              publish-rate (/ events (/ elapsed 1000.0))
              delivery-rate (/ (* events subscribers) (/ elapsed 1000.0))]
          {:elapsed-ms elapsed
           :completed? (if completed? 1.0 0.0)
           :publishes-per-sec publish-rate
           :deliveries-per-sec delivery-rate}))
      (finally
        (bus/close bus)))))

(defn- buffered-backpressure-run [{:keys [events payload-bytes drain-timeout-ms] :as opts}]
  (let [bus (make-bench-bus (assoc opts :mode :buffered))
        failures (atom 0)
        accepted (atom 0)]
    (try
      (bus/subscribe bus :bench/event (fn [_ _] (Thread/sleep 5)))
      (let [start (now-ns)]
        (dotimes [i events]
          (try
            (bus/publish bus :bench/event (build-payload payload-bytes i) {:module :bench})
            (swap! accepted inc)
            (catch IllegalStateException _
              (swap! failures inc))))
        (Thread/sleep (min drain-timeout-ms 250))
        {:elapsed-ms (nanos->ms (- (now-ns) start))
         :attempts events
         :accepted @accepted
         :failures @failures
         :failure-rate (if (zero? events) 0.0 (/ @failures (double events)))} )
      (finally
        (bus/close bus)))))

(defn- buffered-drain-run [{:keys [events payload-bytes drain-timeout-ms] :as opts}]
  (let [bus (make-bench-bus (assoc opts :mode :buffered))
        accepted (atom 0)
        failures (atom 0)
        latch (CountDownLatch. events)]
    (try
      (bus/subscribe bus :bench/event
                     (fn [_ _]
                       (Thread/sleep 2)
                       (.countDown latch)))
      (let [fill-start (now-ns)]
        (dotimes [i events]
          (try
            (bus/publish bus :bench/event (build-payload payload-bytes i) {:module :bench})
            (swap! accepted inc)
            (catch IllegalStateException _
              (swap! failures inc))))
        (let [drain-start (now-ns)
              completed? (await-latch latch drain-timeout-ms)]
          {:fill-elapsed-ms (nanos->ms (- drain-start fill-start))
           :drain-elapsed-ms (nanos->ms (- (now-ns) drain-start))
           :accepted @accepted
           :failures @failures
           :completed? (if completed? 1.0 0.0)}))
      (finally
        (bus/close bus)))))

(defn- publish-memory-run [{:keys [events subscribers payload-bytes] :as opts}]
  (let [bus (make-bench-bus opts)
        latch (CountDownLatch. (* events subscribers))]
    (try
      (dotimes [_ subscribers]
        (bus/subscribe bus :bench/event (fn [_ _] (.countDown latch))))
      (settle-memory!)
      (let [before (capture-memory)]
        (dotimes [i events]
          (bus/publish bus :bench/event (build-payload payload-bytes i) {:module :bench}))
        (await-latch latch 5000)
        (settle-memory!)
        (let [after (capture-memory)
              summary (memory-summary before after)]
          (assoc summary
                 :events events
                 :subscribers subscribers
                 :payload-bytes payload-bytes
                 :heap-delta-per-event-bytes
                 (if (pos? events)
                   (/ (* 1024.0 (:heap-delta-kb summary)) (double events))
                   0.0))))
      (finally
        (bus/close bus)))))

(defn- buffered-memory-pressure-run [{:keys [events payload-bytes drain-timeout-ms] :as opts}]
  (let [bus (make-bench-bus (assoc opts :mode :buffered))
        accepted (atom 0)
        failures (atom 0)]
    (try
      (bus/subscribe bus :bench/event (fn [_ _] (Thread/sleep 5)))
      (settle-memory!)
      (let [before (capture-memory)]
        (dotimes [i events]
          (try
            (bus/publish bus :bench/event (build-payload payload-bytes i) {:module :bench})
            (swap! accepted inc)
            (catch IllegalStateException _
              (swap! failures inc))))
        (Thread/sleep (min drain-timeout-ms 250))
        (settle-memory!)
        (let [after (capture-memory)
              summary (memory-summary before after)]
          (assoc summary
                 :attempts events
                 :accepted @accepted
                 :failures @failures
                 :heap-delta-per-accepted-event-bytes
                 (if (pos? @accepted)
                   (/ (* 1024.0 (:heap-delta-kb summary)) (double @accepted))
                   0.0))))
      (finally
        (bus/close bus)))))

(defn- transact-throughput-run [{:keys [tx-count tx-batch backend handler-backoff-ms handler-max-retries tx-timeout-ms] :as opts}]
  (let [bus (make-bench-bus {:tx-store (make-tx-store backend opts)
                             :handler-backoff-ms handler-backoff-ms
                             :handler-max-retries handler-max-retries})]
    (try
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
        (let [elapsed (nanos->ms (- (now-ns) start))]
          {:elapsed-ms elapsed
           :tx-per-sec (/ tx-count (/ elapsed 1000.0))
           :events-per-sec (/ (* tx-count tx-batch) (/ elapsed 1000.0))
           :timeouts @timeouts}))
      (finally
        (bus/close bus)))))

(defn- transact-latency-run [{:keys [latency-samples tx-batch backend handler-backoff-ms handler-max-retries tx-timeout-ms] :as opts}]
  (let [bus (make-bench-bus {:tx-store (make-tx-store backend opts)
                             :handler-backoff-ms handler-backoff-ms
                             :handler-max-retries handler-max-retries})]
    (try
      (bus/subscribe bus :bench/tx (fn [_ _] true))
      (let [latencies (volatile! [])
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
                  (vswap! latencies conj (nanos->ms (- (now-ns) start))))))))
        (assoc (stats @latencies) :timeouts @timeouts))
      (finally
        (bus/close bus)))))

(def ^:private scenarios
  [{:name :publish-call-latency
    :group :publish
    :scenario-fn publish-call-latency-run}
   {:name :publish-end-to-end-latency
    :group :publish
    :scenario-fn publish-end-to-end-latency-run}
   {:name :publish-throughput
    :group :publish
    :scenario-fn publish-throughput-run}
   {:name :buffered-backpressure
    :group :buffered
    :scenario-fn buffered-backpressure-run
    :modes #{:buffered}}
   {:name :buffered-drain-behavior
    :group :buffered
    :scenario-fn buffered-drain-run
    :modes #{:buffered}}
   {:name :publish-memory-baseline
    :group :memory
    :scenario-fn publish-memory-run}
   {:name :publish-memory-payload
    :group :memory
    :scenario-fn (fn [opts]
                   (publish-memory-run (update opts :payload-bytes #(max % 1024))))}
   {:name :buffered-memory-pressure
    :group :memory
    :scenario-fn buffered-memory-pressure-run
    :modes #{:buffered}}
   {:name :transact-throughput
    :group :transact
    :scenario-fn transact-throughput-run}
   {:name :transact-latency
    :group :transact
    :scenario-fn transact-latency-run}])

(defn- scenario-enabled? [{:keys [group name modes]} {:keys [only mode scenario]}]
  (and
   (or (nil? scenario) (= scenario name))
   (or (= only :all) (= only group))
   (or (nil? modes) (contains? modes mode))))

(defn- selected-scenarios [opts]
  (vec (filter #(scenario-enabled? % opts) scenarios)))

(defn- print-section [title data]
  (println)
  (println title)
  (println (pr-str data)))

(defn- run-selected-scenarios [opts]
  (doseq [{scenario-name :name scenario-fn :scenario-fn} (selected-scenarios opts)]
    (print-section
     (str "SCENARIO " (clojure.core/name scenario-name))
     (:summary (run-scenario* (assoc opts :name scenario-name :scenario-fn scenario-fn))))))

(defn -main [& args]
  (let [opts (parse-args args)]
    (when (= (:backend opts) :datahike)
      (timbre/merge-config! {:min-level :warn}))
    (print-section "BENCH OPTIONS" opts)
    (let [scenarios-to-run (selected-scenarios opts)]
      (when (empty? scenarios-to-run)
        (throw (IllegalArgumentException.
                (str "No scenarios selected for options: " opts))))
      (run-selected-scenarios opts))))
