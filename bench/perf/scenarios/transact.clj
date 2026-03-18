(ns perf.scenarios.transact
  (:require [event-bus :as bus]
            [perf.bus :as bench-bus]
            [perf.summary :as summary]
            [perf.util :as util]))

(defn transact-throughput-run [{:keys [tx-count tx-batch backend handler-backoff-ms handler-max-retries tx-timeout-ms] :as opts}]
  (let [bus (bench-bus/make-bench-bus {:tx-store (bench-bus/make-tx-store backend opts)
                                       :handler-backoff-ms handler-backoff-ms
                                       :handler-max-retries handler-max-retries})]
    (try
      (bus/subscribe bus :bench/tx (fn [_ _] true))
      (let [start (util/now-ns)
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
        (let [elapsed (util/nanos->ms (- (util/now-ns) start))]
          {:elapsed-ms elapsed
           :tx-per-sec (/ tx-count (/ elapsed 1000.0))
           :events-per-sec (/ (* tx-count tx-batch) (/ elapsed 1000.0))
           :timeouts @timeouts}))
      (finally
        (bus/close bus)))))

(defn transact-latency-run [{:keys [latency-samples tx-batch backend handler-backoff-ms handler-max-retries tx-timeout-ms] :as opts}]
  (let [bus (bench-bus/make-bench-bus {:tx-store (bench-bus/make-tx-store backend opts)
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
                start (util/now-ns)
                tx-result (deref (future (bus/transact bus events))
                                 tx-timeout-ms
                                 ::timeout)]
            (if (= tx-result ::timeout)
              (swap! timeouts inc)
              (let [{:keys [result-promise]} tx-result
                    result (deref result-promise tx-timeout-ms ::timeout)]
                (if (= result ::timeout)
                  (swap! timeouts inc)
                  (vswap! latencies conj (util/nanos->ms (- (util/now-ns) start))))))))
        (assoc (summary/stats @latencies) :timeouts @timeouts))
      (finally
        (bus/close bus)))))
