(ns transact-filelog-test
  (:require [clojure.core.async :as async]
            [clojure.test :refer [deftest is]]
            [event-bus :as bus]
            [malli.core :as m])
  (:import [java.io File]))

(def any-schema (m/schema :any))

(def test-registry
  {:test/event {"1.0" any-schema}})

(defn make-test-bus [& {:as opts}]
  (apply bus/make-bus (apply concat (merge {:schema-registry test-registry} opts))))

(defn- make-filelog-config []
  (let [^File file (File/createTempFile "event-bus-filelog-" ".log")]
    (.deleteOnExit file)
    {:path (.getAbsolutePath file)}))

(defn make-tx-store []
  {:db/type :filelog
   :filelog/config (make-filelog-config)})

(defn- run-with-timeout!
  [timeout-ms f]
  (let [result (deref (future (f)) timeout-ms ::timeout)]
    (when (= result ::timeout)
      (is false (str "Filelog test timed out after " timeout-ms " ms")))))

(deftest transact-success-filelog-test
  (run-with-timeout!
   120000
   (fn []
     (let [bus (make-test-bus :tx-store (make-tx-store))
           called (atom 0)]
       (bus/subscribe bus :test/event (fn [_ _] (swap! called inc) true))
       (let [{:keys [result-promise result-chan]}
             (bus/transact bus [{:event-type :test/event
                                 :payload {:data 1}
                                 :module :test/tx}])
             result (deref result-promise 2000 ::timeout)
             chan-result (async/<!! result-chan)]
         (is (not= result ::timeout))
         (is (:ok? result))
         (is (:ok? chan-result))
         (is (= 1 @called)))
       (bus/close bus)))))

(deftest transact-handler-failure-filelog-test
  (run-with-timeout!
   120000
   (fn []
     (let [bus (make-test-bus :tx-store (make-tx-store) :handler-max-retries 1)
           called (atom 0)]
       (bus/subscribe bus :test/event (fn [_ _] (swap! called inc) false))
       (let [{:keys [result-promise]}
             (bus/transact bus [{:event-type :test/event
                                 :payload {:data 1}
                                 :module :test/tx}])
             result (deref result-promise 2000 ::timeout)]
         (is (not= result ::timeout))
         (is (false? (:ok? result)))
         (is (= 1 @called)))
       (bus/close bus)))))

(deftest transact-timeout-filelog-test
  (run-with-timeout!
   120000
   (fn []
     (let [bus (make-test-bus :tx-store (make-tx-store)
                              :tx-handler-timeout 10
                              :handler-max-retries 1)
           called (atom 0)]
       (bus/subscribe bus :test/event
                      (fn [_ _]
                        (swap! called inc)
                        (Thread/sleep 50)
                        true))
       (let [{:keys [result-promise]}
             (bus/transact bus [{:event-type :test/event
                                 :payload {:data 1}
                                 :module :test/tx}])
             result (deref result-promise 2000 ::timeout)]
         (is (not= result ::timeout))
         (is (false? (:ok? result)))
         (is (= 1 @called)))
       (bus/close bus)))))

(deftest transact-retry-success-filelog-test
  (run-with-timeout!
   120000
   (fn []
     (let [bus (make-test-bus :tx-store (make-tx-store)
                              :handler-max-retries 2
                              :handler-backoff-ms 10)
           called (atom 0)]
       (bus/subscribe bus :test/event
                      (fn [_ _]
                        (let [n (swap! called inc)]
                          (if (= n 1) false true))))
       (let [{:keys [result-promise]}
             (bus/transact bus [{:event-type :test/event
                                 :payload {:data 1}
                                 :module :test/tx}])
             result (deref result-promise 3000 ::timeout)]
         (is (not= result ::timeout))
         (is (:ok? result))
         (is (= 2 @called)))
       (bus/close bus)))))

(deftest transact-recovery-filelog-test
  (run-with-timeout!
   120000
   (fn []
     (let [config (make-filelog-config)
           path (:path config)
           called (atom 0)
           bus1 (make-test-bus :tx-store {:db/type :filelog :filelog/config {:path path}})]
       (bus/subscribe bus1 :test/event (fn [_ _] (swap! called inc) true))
       (let [{:keys [result-promise]}
             (bus/transact bus1 [{:event-type :test/event
                                  :payload {:data 1}
                                  :module :test/tx}])
             result (deref result-promise 2000 ::timeout)]
         (is (not= result ::timeout))
         (is (:ok? result))
         (is (= 1 @called)))
       (bus/close bus1)
       (let [called2 (atom 0)
             bus2 (make-test-bus :tx-store {:db/type :filelog :filelog/config {:path path}})]
         (bus/subscribe bus2 :test/event (fn [_ _] (swap! called2 inc) true))
         (Thread/sleep 200)
         (is (= 0 @called2))
         (bus/close bus2))))))
