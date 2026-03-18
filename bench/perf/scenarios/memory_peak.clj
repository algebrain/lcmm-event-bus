(ns perf.scenarios.memory-peak
  (:require [event-bus :as bus]
            [perf.bus :as bench-bus]
            [perf.util :as util]))

(defn- start-sampler! [peak* stop?]
  (future
    (while (not @stop?)
      (swap! peak* max (util/used-heap-bytes))
      (Thread/sleep 5))))

(defn publish-peak-burst-run [{:keys [events payload-bytes drain-timeout-ms] :as opts}]
  (let [bus (bench-bus/make-bench-bus opts)
        peak* (atom 0)
        stop? (atom false)
        baseline (util/used-heap-bytes)
        sampler (start-sampler! peak* stop?)]
    (try
      (bus/subscribe bus :bench/event (fn [_ _] nil))
      (dotimes [i events]
        (bus/publish bus :bench/event (util/build-payload payload-bytes i) {:module :bench}))
      (Thread/sleep (min drain-timeout-ms 250))
      {:baseline-used-heap-kb (util/bytes->kb baseline)
       :peak-used-heap-kb (util/bytes->kb @peak*)
       :peak-delta-kb (util/bytes->kb (- @peak* baseline))}
      (finally
        (reset! stop? true)
        @sampler
        (bus/close bus)))))

(defn buffered-peak-pressure-run [{:keys [events payload-bytes drain-timeout-ms] :as opts}]
  (let [bus (bench-bus/make-bench-bus (assoc opts :mode :buffered))
        peak* (atom 0)
        stop? (atom false)
        accepted (atom 0)
        failures (atom 0)
        baseline (util/used-heap-bytes)
        sampler (start-sampler! peak* stop?)]
    (try
      (bus/subscribe bus :bench/event (fn [_ _] (Thread/sleep 5)))
      (dotimes [i events]
        (try
          (bus/publish bus :bench/event (util/build-payload payload-bytes i) {:module :bench})
          (swap! accepted inc)
          (catch IllegalStateException _
            (swap! failures inc))))
      (Thread/sleep (min drain-timeout-ms 250))
      {:accepted @accepted
       :failures @failures
       :baseline-used-heap-kb (util/bytes->kb baseline)
       :peak-used-heap-kb (util/bytes->kb @peak*)
       :peak-delta-kb (util/bytes->kb (- @peak* baseline))}
      (finally
        (reset! stop? true)
        @sampler
        (bus/close bus)))))
