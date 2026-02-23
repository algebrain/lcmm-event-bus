(ns publish-test
  (:require [clojure.test :refer [deftest is]]
            [event-bus :as bus]
            [malli.core :as m]
            [support :as support])
  (:import [java.util UUID]
           [java.util.concurrent CountDownLatch]))

(deftest basic-publication-and-subscription-test
  (let [bus (support/make-test-bus)
        latch (CountDownLatch. 1)
        received (atom nil)]
    (bus/subscribe bus :test/event
                   (fn [_bus envelope]
                     (reset! received envelope)
                     (.countDown latch)))
    (let [published-envelope (bus/publish bus :test/event {:data 42} {:module :test/basic})]
      (is (some? (:correlation-id published-envelope)) "Published envelope should have a correlation-id")
      (is (instance? UUID (:correlation-id published-envelope)) "correlation-id should be a UUID")
      (is (some? (:message-id published-envelope)) "Published envelope should have a message-id")
      (is (instance? UUID (:message-id published-envelope)) "message-id should be a UUID"))
    (is (support/await-on-latch latch) "Handler should have been called")
    (is (= :test/event (:message-type @received)))
    (is (= {:data 42} (:payload @received)))
    (bus/close bus)))

(deftest multiple-subscribers-test
  (let [bus (support/make-test-bus)
        latch (CountDownLatch. 3)
        results (atom [])]
    (dotimes [_ 3]
      (bus/subscribe bus :multi/event
                     (fn [_bus envelope]
                       (swap! results conj (:payload envelope))
                       (.countDown latch))))
    (bus/publish bus :multi/event 1 {:module :test/multi})
    (is (support/await-on-latch latch) "All 3 handlers should have been called")
    (is (= [1 1 1] (sort @results)))
    (bus/close bus)))

(deftest handler-error-isolation-test
  (let [bus (support/make-test-bus)
        latch (CountDownLatch. 1)
        good-handler-called (atom false)]
    (bus/subscribe bus :test/event (fn [_ _] (throw (RuntimeException. "FAIL"))))
    (bus/subscribe bus :test/event (fn [_ _] (reset! good-handler-called true) (.countDown latch)))

    (bus/publish bus :test/event nil {:module :test/handler})
    (is (support/await-on-latch latch) "Good handler should run despite the other failing")
    (is (true? @good-handler-called))
    (bus/close bus)))

(deftest publish-schema-validation-test
  (let [log-atom (atom [])
        bus (support/make-test-bus :logger (fn [level data] (swap! log-atom conj (assoc data :level level))))
        schema-handler-called (atom false)]
    (bus/subscribe bus :schema/event
                   (fn [_ _] (reset! schema-handler-called true))
                   {:schema (m/schema [:map [:x :int]])})
    (is (thrown? IllegalStateException
                 (bus/publish bus :schema/event {:x "not-int"} {:module :test/schema})))
    (is (false? @schema-handler-called) "Handlers should not run when publish validation fails")
    (is (some #(= :publish-schema-validation-failed (:event %)) @log-atom)
        "Publish validation failure should be logged")
    (bus/close bus)))

(deftest publish-schema-missing-test
  (let [bus (support/make-test-bus :schema-registry {})]
    (is (thrown? IllegalStateException
                 (bus/publish bus :missing/event {} {:module :test/missing})))
    (bus/close bus)))

(deftest publish-schema-version-selection-test
  (let [bus (support/make-test-bus)]
    (let [env-v1 (bus/publish bus :versioned/event {:v 1} {:module :test/version})]
      (is (= "1.0" (:schema-version env-v1))))
    (let [env-v2 (bus/publish bus :versioned/event {:v "ok"} {:module :test/version :schema-version "2.0"})]
      (is (= "2.0" (:schema-version env-v2))))
    (is (thrown? IllegalStateException
                 (bus/publish bus :versioned/event {:v 2} {:module :test/version :schema-version "2.0"})))
    (bus/close bus)))
