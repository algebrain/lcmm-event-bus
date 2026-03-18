(ns perf.scenarios.memory-retained
  (:require [event-bus :as bus]
            [perf.bus :as bench-bus]
            [perf.util :as util])
  (:import [java.util.concurrent CountDownLatch]))

(defn- fanout-subscribers
  [subscribers]
  (max 8 (long (or subscribers 1))))

(defn publish-memory-baseline-run [{:keys [events subscribers payload-bytes drain-timeout-ms] :as opts}]
  (let [bus (bench-bus/make-bench-bus opts)
        latch (CountDownLatch. (* events subscribers))]
    (try
      (dotimes [_ subscribers]
        (bus/subscribe bus :bench/event (fn [_ _] (.countDown latch))))
      (util/settle-memory!)
      (let [before (util/capture-memory)]
        (dotimes [i events]
          (bus/publish bus :bench/event (util/build-payload payload-bytes i) {:module :bench}))
        (let [completed? (util/await-latch latch drain-timeout-ms)]
          (util/settle-memory!)
          (let [after (util/capture-memory)
                summary (util/memory-summary before after)]
            (assoc summary
                   :events events
                   :subscribers subscribers
                   :payload-bytes payload-bytes
                   :completed? (if completed? 1.0 0.0)
                   :heap-delta-per-event-bytes
                   (if (pos? events)
                     (/ (* 1024.0 (:heap-delta-kb summary)) (double events))
                     0.0)))))
      (finally
        (bus/close bus)))))

(defn publish-memory-payload-run [opts]
  (publish-memory-baseline-run (update opts :payload-bytes #(max % 1024))))

(defn buffered-memory-pressure-run [{:keys [events payload-bytes drain-timeout-ms] :as opts}]
  (let [bus (bench-bus/make-bench-bus (assoc opts :mode :buffered))
        accepted (atom 0)
        failures (atom 0)]
    (try
      (bus/subscribe bus :bench/event (fn [_ _] (Thread/sleep 5)))
      (util/settle-memory!)
      (let [before (util/capture-memory)]
        (dotimes [i events]
          (try
            (bus/publish bus :bench/event (util/build-payload payload-bytes i) {:module :bench})
            (swap! accepted inc)
            (catch IllegalStateException _
              (swap! failures inc))))
        (Thread/sleep (min drain-timeout-ms 250))
        (util/settle-memory!)
        (let [after (util/capture-memory)
              summary (util/memory-summary before after)]
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

(defn buffered-memory-pressure-fanout-run [{:keys [events payload-bytes drain-timeout-ms subscribers] :as opts}]
  (let [bus (bench-bus/make-bench-bus (assoc opts :mode :buffered))
        subscribers (fanout-subscribers subscribers)
        accepted (atom 0)
        failures (atom 0)]
    (try
      (dotimes [_ subscribers]
        (bus/subscribe bus :bench/event (fn [_ _] (Thread/sleep 5))))
      (util/settle-memory!)
      (let [before (util/capture-memory)]
        (dotimes [i events]
          (try
            (bus/publish bus :bench/event (util/build-payload payload-bytes i) {:module :bench})
            (swap! accepted inc)
            (catch IllegalStateException _
              (swap! failures inc))))
        (Thread/sleep (min drain-timeout-ms 250))
        (util/settle-memory!)
        (let [after (util/capture-memory)
              summary (util/memory-summary before after)]
          (assoc summary
                 :attempts events
                 :accepted @accepted
                 :failures @failures
                 :subscribers subscribers
                 :heap-delta-per-accepted-event-bytes
                 (if (pos? @accepted)
                   (/ (* 1024.0 (:heap-delta-kb summary)) (double @accepted))
                   0.0))))
      (finally
        (bus/close bus)))))

(def child-scenarios
  {:publish-memory-baseline publish-memory-baseline-run
   :publish-memory-payload publish-memory-payload-run
   :buffered-memory-pressure buffered-memory-pressure-run
   :buffered-memory-pressure-fanout buffered-memory-pressure-fanout-run})
