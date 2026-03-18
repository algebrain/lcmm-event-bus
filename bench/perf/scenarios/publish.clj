(ns perf.scenarios.publish
  (:require [event-bus :as bus]
            [perf.bus :as bench-bus]
            [perf.summary :as summary]
            [perf.util :as util])
  (:import [java.util.concurrent CountDownLatch]))

(defn publish-call-latency-run [{:keys [events payload-bytes] :as opts}]
  (let [bus (bench-bus/make-bench-bus opts)
        durations (double-array events)]
    (try
      (dotimes [i events]
        (let [payload (util/build-payload payload-bytes i)
              start (util/now-ns)]
          (bus/publish bus :bench/event payload {:module :bench})
          (aset-double durations i (util/nanos->ms (- (util/now-ns) start)))))
      (summary/stats (vec durations))
      (finally
        (bus/close bus)))))

(defn publish-end-to-end-latency-run [{:keys [events payload-bytes drain-timeout-ms] :as opts}]
  (let [bus (bench-bus/make-bench-bus opts)
        latch (CountDownLatch. events)
        durations (double-array events)
        delivered (boolean-array events)]
    (try
      (bus/subscribe bus :bench/event
                     (fn [_ envelope]
                       (let [idx (int (get-in envelope [:payload :idx]))
                             delta (util/nanos->ms (- (util/now-ns) (long (get-in envelope [:payload :start-ns]))))]
                         (aset-double durations idx delta)
                         (aset-boolean delivered idx true)
                         (.countDown latch))))
      (dotimes [i events]
        (bus/publish bus
                     :bench/event
                     (assoc (util/build-payload payload-bytes i)
                            :idx i
                            :start-ns (util/now-ns))
                     {:module :bench}))
      (let [completed? (util/await-latch latch drain-timeout-ms)
            delivered-values (vec (keep-indexed (fn [idx value]
                                                  (when (aget delivered idx)
                                                    value))
                                                (vec durations)))
            delivered-count (count delivered-values)]
        (assoc (summary/stats delivered-values)
               :delivered delivered-count
               :expected events
               :completion-rate (if (zero? events)
                                  1.0
                                  (/ delivered-count (double events)))
               :completed? (if completed? 1.0 0.0)))
      (finally
        (bus/close bus)))))

(defn publish-throughput-run [{:keys [events subscribers payload-bytes drain-timeout-ms] :as opts}]
  (let [bus (bench-bus/make-bench-bus opts)
        latch (CountDownLatch. (* events subscribers))]
    (try
      (dotimes [_ subscribers]
        (bus/subscribe bus :bench/event (fn [_ _] (.countDown latch))))
      (let [start (util/now-ns)]
        (dotimes [i events]
          (bus/publish bus :bench/event (util/build-payload payload-bytes i) {:module :bench}))
        (let [completed? (util/await-latch latch drain-timeout-ms)
              elapsed (util/nanos->ms (- (util/now-ns) start))
              publish-rate (/ events (/ elapsed 1000.0))
              delivery-rate (/ (* events subscribers) (/ elapsed 1000.0))]
          {:elapsed-ms elapsed
           :completed? (if completed? 1.0 0.0)
           :publishes-per-sec publish-rate
           :deliveries-per-sec delivery-rate}))
      (finally
        (bus/close bus)))))
