(ns logging-test
  (:require [clojure.edn :as edn]
            [clojure.test :refer [deftest is testing]]
            [event-bus :as bus]
            [malli.core :as m]
            [support :as support])
  (:import [java.util.concurrent CountDownLatch]))

(deftest logger-exception-breaks-publish-test
  (let [bus (support/make-test-bus :logger (fn [_level data]
                                             (when (= :event-published (:event data))
                                               (throw (RuntimeException. "LOGGER FAIL")))))]
    (is (true? (try
                 (bus/publish bus :log/break nil {:module :test/log})
                 true
                 (catch RuntimeException _
                   false)))
        "Logger failure should not break publish")
    (bus/close bus)))

(deftest logger-and-schema-validation-test
  (let [log-atom (atom [])
        bus (support/make-test-bus :logger (fn [level data] (swap! log-atom conj (assoc data :level level))))
        latch (CountDownLatch. 1)
        good-schema (m/schema [:map [:x :int]])]

    (bus/subscribe bus :log/event (fn [_ _] (throw (RuntimeException. "FAIL"))) {:schema good-schema})
    (bus/subscribe bus :log/event (fn [_ _] (.countDown latch)) {:schema good-schema})
    (bus/subscribe bus :log/event (fn [_ _] (.countDown latch)) {:schema (m/schema [:map [:y :string]])})

    (bus/publish bus :log/event {:x 42} {:module :test/log})
    (is (support/await-on-latch latch) "Handlers should complete")
    (bus/close bus)

    (let [logs @log-atom]
      (testing "Schema validation failure is logged"
        (is (some #(and (= :warn (:level %))
                        (= :schema-validation-failed (:event %))
                        (= :log/event (:event-type %)))
                  logs)))
      (testing "Handler failure is logged"
        (is (some #(and (= :error (:level %))
                        (= :handler-failed (:event %))
                        (instance? RuntimeException (:exception %)))
                  logs)))
      (testing "Successful publication is logged"
        (is (some #(= :event-published (:event %)) logs))))))

(deftest log-payload-none-test
  (let [log-atom (atom [])
        bus (support/make-test-bus :logger (fn [_level data] (swap! log-atom conj data))
                                   :log-payload :none)]
    (bus/subscribe bus :test/event (fn [_ _] (throw (RuntimeException. "FAIL"))))
    (bus/publish bus :test/event {:secret "x"} {:module :test/log})
    (Thread/sleep 200)
    (let [entry (first (filter #(= :handler-failed (:event %)) @log-atom))]
      (is (some? entry))
      (is (not (contains? entry :payload))))
    (bus/close bus)))

(deftest log-payload-keys-test
  (let [log-atom (atom [])
        bus (support/make-test-bus :logger (fn [_level data] (swap! log-atom conj data))
                                   :log-payload :keys)]
    (is (thrown? IllegalStateException
                 (bus/publish bus :schema/event {:x "not-int"} {:module :test/schema})))
    (let [entry (first (filter #(= :publish-schema-validation-failed (:event %)) @log-atom))]
      (is (some? entry))
      (is (= #{:x} (set (:payload entry)))))
    (bus/close bus)))

(deftest log-payload-dump-test
  (let [log-atom (atom [])
        dir (support/temp-dir)
        bus (support/make-test-bus :logger (fn [_level data] (swap! log-atom conj data))
                                   :log-payload :none
                                   :payload-dump {:on-events #{:handler-failed}
                                                  :dir (.getAbsolutePath dir)})]
    (bus/subscribe bus :test/event (fn [_ _] (throw (RuntimeException. "FAIL"))))
    (bus/publish bus :test/event {:secret "x"} {:module :test/log})
    (is (support/wait-until 2000 #(seq (.listFiles dir))))
    (let [files (.listFiles dir)
          content (slurp (first files))
          dumped (edn/read-string content)]
      (is (= {:secret "x"} (:payload dumped))))
    (bus/close bus)))
