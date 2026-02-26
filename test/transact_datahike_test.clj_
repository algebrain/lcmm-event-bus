(ns transact-datahike-test
  (:require [clojure.core.async :as async]
            [clojure.test :refer [deftest is]]
            [event-bus :as bus]
            [support :as support]))

(deftest transact-success-test
  (support/run-with-timeout!
   120000
   (fn []
     (let [bus (support/make-test-bus :tx-store (support/make-tx-store :datahike))
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

(deftest transact-handler-failure-test
  (support/run-with-timeout!
   120000
   (fn []
     (let [bus (support/make-test-bus :tx-store (support/make-tx-store :datahike) :handler-max-retries 1)
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

(deftest transact-timeout-test
  (support/run-with-timeout!
   120000
   (fn []
     (let [bus (support/make-test-bus :tx-store (support/make-tx-store :datahike)
                                      :tx-handler-timeout 10
                                      :handler-max-retries 1)
           called (atom 0)]
       (bus/subscribe bus :test/event
                      (fn [_ _]
                        (swap! called inc)
                        (Thread/sleep 50)
                        true))
       (let [{:keys [result-promise]} (bus/transact bus
                                                    [{:event-type :test/event
                                                      :payload {:data 1}
                                                      :module :test/tx}])
             result (deref result-promise 2000 ::timeout)]
         (is (not= result ::timeout))
         (is (false? (:ok? result)))
         (is (= 1 @called)))
       (bus/close bus)))))

(deftest transact-retry-success-test
  (support/run-with-timeout!
   120000
   (fn []
     (let [bus (support/make-test-bus :tx-store (support/make-tx-store :datahike)
                                      :handler-max-retries 2
                                      :handler-backoff-ms 10)
           called (atom 0)]
       (bus/subscribe bus :test/event
                      (fn [_ _]
                        (let [n (swap! called inc)]
                          (if (= n 1) false true))))
       (let [{:keys [result-promise]} (bus/transact bus
                                                    [{:event-type :test/event
                                                      :payload {:data 1}
                                                      :module :test/tx}])
             result (deref result-promise 3000 ::timeout)]
         (is (not= result ::timeout))
         (is (:ok? result))
         (is (= 2 @called)))
       (bus/close bus)))))
