(ns perf.scenarios.buffered
  (:require [event-bus :as bus]
            [perf.bus :as bench-bus]
            [perf.util :as util]))

(defn- fanout-subscribers
  [subscribers]
  (max 8 (long (or subscribers 1))))

(defn- subscribe-slow-handlers!
  [bus subscribers]
  (dotimes [_ subscribers]
    (bus/subscribe bus :bench/event (fn [_ _] (Thread/sleep 5)))))

(defn buffered-backpressure-run [{:keys [events payload-bytes drain-timeout-ms subscribers] :as opts}]
  (let [bus (bench-bus/make-bench-bus (assoc opts :mode :buffered))
        failures (atom 0)
        accepted (atom 0)]
    (try
      (subscribe-slow-handlers! bus (or subscribers 1))
      (let [start (util/now-ns)]
        (dotimes [i events]
          (try
            (bus/publish bus :bench/event (util/build-payload payload-bytes i) {:module :bench})
            (swap! accepted inc)
            (catch IllegalStateException _
              (swap! failures inc))))
        (Thread/sleep (min drain-timeout-ms 250))
        {:elapsed-ms (util/nanos->ms (- (util/now-ns) start))
         :attempts events
         :accepted @accepted
         :failures @failures
         :failure-rate (if (zero? events) 0.0 (/ @failures (double events)))
         :subscribers (or subscribers 1)})
      (finally
        (bus/close bus)))))

(defn buffered-drain-run [{:keys [events payload-bytes drain-timeout-ms subscribers] :as opts}]
  (let [bus (bench-bus/make-bench-bus (assoc opts :mode :buffered))
        subscribers (or subscribers 1)
        accepted (atom 0)
        failures (atom 0)
        processed (atom 0)]
    (try
      (dotimes [_ subscribers]
        (bus/subscribe bus :bench/event
                       (fn [_ _]
                         (Thread/sleep 2)
                         (swap! processed inc))))
      (let [fill-start (util/now-ns)]
        (dotimes [i events]
          (try
            (bus/publish bus :bench/event (util/build-payload payload-bytes i) {:module :bench})
            (swap! accepted inc)
            (catch IllegalStateException _
              (swap! failures inc))))
        (let [drain-start (util/now-ns)
              expected (* @accepted subscribers)
              completed? (util/await-condition drain-timeout-ms #(= @processed expected))]
          {:fill-elapsed-ms (util/nanos->ms (- drain-start fill-start))
           :drain-elapsed-ms (util/nanos->ms (- (util/now-ns) drain-start))
           :accepted @accepted
           :failures @failures
           :processed @processed
           :subscribers subscribers
           :completed? (if completed? 1.0 0.0)}))
      (finally
        (bus/close bus)))))

(defn buffered-backpressure-fanout-run [opts]
  (buffered-backpressure-run (update opts :subscribers fanout-subscribers)))

(defn buffered-drain-fanout-run [opts]
  (buffered-drain-run (update opts :subscribers fanout-subscribers)))
