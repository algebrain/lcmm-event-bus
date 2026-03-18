(ns bench-perf-test
  (:require [clojure.test :refer [deftest is]]
            [event-bus]
            [perf.scenario-registry :as registry]
            [perf.scenarios.buffered :as buffered]
            [perf.scenarios.memory-retained :as memory-retained]
            [perf.scenarios.publish :as publish]
            [perf.summary :as summary]
            [perf.util :as util]))

(deftest buffered-scenarios-do-not-depend-on-external-mode-test
  (let [scenario-names (mapv :name (registry/selected-scenarios {:only :buffered
                                                                 :mode :unlimited}))]
    (is (= [:buffered-backpressure :buffered-drain-behavior] scenario-names))))

(deftest memory-kind-selection-test
  (is (= [:publish-memory-baseline :publish-memory-payload :buffered-memory-pressure]
         (mapv :name (registry/selected-scenarios {:only :memory
                                                   :mode :buffered
                                                   :memory-kind :retained}))))
  (is (= [:publish-peak-burst :buffered-peak-pressure]
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
