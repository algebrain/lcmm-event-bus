(ns perf.cli
  (:require [clojure.string :as str]))

(def default-opts
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
   :fsync-interval-ms nil
   :memory-isolated? true
   :memory-kind :retained})

(def quick-defaults
  {:events 1000
   :latency-samples 250
   :tx-count 30
   :payload-bytes 64
   :warmup-iterations 1
   :measure-iterations 1
   :duration-ms 1000
   :drain-timeout-ms 1500})

(def known-groups
  #{:all :publish :buffered :memory :transact})

(def known-memory-kinds
  #{:retained :peak :all})

(defn parse-long* [s]
  (try
    (Long/parseLong s)
    (catch Throwable _ nil)))

(defn parse-bool* [s]
  (case (some-> s str/lower-case)
    "true" true
    "false" false
    nil))

(defn parse-flagged-args [args]
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

(defn parse-args [args]
  (let [{:keys [flags kv]} (parse-flagged-args args)
        quick? (contains? flags :quick)
        explicit-keys (set (keys kv))
        mode (some-> (get kv :mode) keyword)
        backend (some-> (get kv :backend) keyword)
        only (some-> (get kv :only) keyword)
        scenario (some-> (get kv :scenario) keyword)
        memory-isolated? (parse-bool* (get kv :memory-isolated))
        memory-kind (some-> (get kv :memory-kind) keyword)
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
                 memory-kind (assoc :memory-kind memory-kind)
                 (some? memory-isolated?) (assoc :memory-isolated? memory-isolated?)
                 quick? (assoc :quick? true))
        merged (if (contains? explicit-keys :only)
                 merged
                 (assoc merged :only :all))]
    (when-not (contains? known-groups (:only merged))
      (throw (IllegalArgumentException.
              (str "Unsupported --only value: " (:only merged)))))
    (when-not (contains? known-memory-kinds (:memory-kind merged))
      (throw (IllegalArgumentException.
              (str "Unsupported --memory-kind value: " (:memory-kind merged)))))
    merged))
