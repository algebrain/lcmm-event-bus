(ns bench-perf-test
  (:require [clojure.test :refer [deftest is]]
            [event-bus]
            [perf.bus]
            [perf.scenario-registry :as registry]
            [perf.scenarios.buffered :as buffered]
            [perf.scenarios.memory-retained :as memory-retained]
            [perf.scenarios.publish :as publish]
            [perf.summary :as summary]
            [perf.util :as util]))

(deftest buffered-scenarios-do-not-depend-on-external-mode-test
  (let [scenario-names (mapv :name (registry/selected-scenarios {:only :buffered
                                                                 :mode :unlimited}))]
    (is (= [:buffered-backpressure
            :buffered-backpressure-fanout
            :buffered-drain-behavior
            :buffered-drain-fanout]
           scenario-names))))

(deftest memory-kind-selection-test
  (is (= [:publish-memory-baseline
          :publish-memory-payload
          :buffered-memory-pressure
          :buffered-memory-pressure-fanout]
         (mapv :name (registry/selected-scenarios {:only :memory
                                                   :mode :buffered
                                                   :memory-kind :retained}))))
  (is (= [:publish-peak-burst
          :buffered-peak-pressure
          :buffered-peak-pressure-fanout]
         (mapv :name (registry/selected-scenarios {:only :memory
                                                   :mode :buffered
                                                   :memory-kind :peak}))))
  (is (= [:publish-peak-burst]
         (mapv :name (registry/selected-scenarios {:only :memory
                                                   :mode :unlimited
                                                   :scenario :publish-peak-burst
                                                   :memory-kind :retained})))))

(deftest buffered-drain-completes-when-all-accepted-events-are-processed-test
  (let [handler* (atom nil)]
    (with-redefs [perf.bus/make-bench-bus (fn [_] ::fake-bus)
                  event-bus/subscribe (fn [_ _ handler & _]
                                        (reset! handler* handler)
                                        ::fake-bus)
                  event-bus/publish (fn [_ _ payload _]
                                      (if (zero? (:n payload))
                                        (do
                                          (@handler* nil payload)
                                          {:ok true})
                                        (throw (IllegalStateException. "buffer full"))))
                  event-bus/close (fn [_] nil)]
      (let [result (buffered/buffered-drain-run {:events 2
                                                 :payload-bytes 8
                                                 :drain-timeout-ms 10})]
        (is (= 1 (:accepted result)))
        (is (= 1 (:failures result)))
        (is (= 1.0 (:completed? result)))))))

(deftest publish-end-to-end-latency-excludes-undelivered-events-from-stats-test
  (let [handler* (atom nil)
        captured-values (atom nil)]
    (with-redefs [perf.bus/make-bench-bus (fn [_] ::fake-bus)
                  event-bus/subscribe (fn [_ _ handler & _]
                                        (reset! handler* handler)
                                        ::fake-bus)
                  event-bus/publish (fn [_ _ payload _]
                                      (when (zero? (:idx payload))
                                        (@handler* nil {:payload payload}))
                                      {:ok true})
                  event-bus/close (fn [_] nil)
                  summary/stats (fn [values]
                                  (reset! captured-values values)
                                  {:count (count values)})]
      (let [result (publish/publish-end-to-end-latency-run {:events 2
                                                            :payload-bytes 8
                                                            :drain-timeout-ms 1})]
        (is (= 0.0 (:completed? result)))
        (is (= 1 (count @captured-values)))))))

(deftest publish-memory-run-uses-configured-drain-timeout-test
  (let [timeouts (atom [])]
    (with-redefs [util/await-latch (fn [_ timeout-ms]
                                     (swap! timeouts conj timeout-ms)
                                     true)]
      (memory-retained/publish-memory-baseline-run {:events 0
                                                    :subscribers 0
                                                    :payload-bytes 8
                                                    :drain-timeout-ms 123}))
    (is (= [123] @timeouts))))

(deftest buffered-fanout-benchmarks-force-meaningful-subscriber-count-test
  (let [captured (atom [])]
    (with-redefs [buffered/buffered-backpressure-run (fn [opts]
                                                       (swap! captured conj [:backpressure (:subscribers opts)])
                                                       {:ok true})
                  buffered/buffered-drain-run (fn [opts]
                                                (swap! captured conj [:drain (:subscribers opts)])
                                                {:ok true})]
      (buffered/buffered-backpressure-fanout-run {:subscribers 2})
      (buffered/buffered-drain-fanout-run {:subscribers 3}))
    (is (= [[:backpressure 8]
            [:drain 8]]
           @captured)
        "Buffered fanout scenarios should enforce a meaningful subscriber count where wrapped")))

(deftest buffered-memory-fanout-benchmark-uses-multiple-subscribers-test
  (let [subscriptions (atom 0)]
    (with-redefs [perf.bus/make-bench-bus (fn [_] ::fake-bus)
                  event-bus/subscribe (fn [_ _ _ & _]
                                        (swap! subscriptions inc)
                                        ::fake-bus)
                  event-bus/publish (fn [_ _ _ _]
                                      nil)
                  event-bus/close (fn [_] nil)
                  util/settle-memory! (fn [] nil)
                  util/capture-memory (fn [] {:used-heap-kb 0})
                  util/memory-summary (fn [_ _] {:heap-delta-kb 0.0})]
      (memory-retained/buffered-memory-pressure-fanout-run {:events 0
                                                            :payload-bytes 8
                                                            :drain-timeout-ms 1
                                                            :subscribers 2}))
    (is (= 8 @subscriptions)
        "Buffered memory fanout scenario should subscribe multiple slow handlers even for low input values")))
