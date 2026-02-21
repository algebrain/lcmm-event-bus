(ns event-bus-test
  (:require [clojure.core.async :as async]
            [clojure.test :refer [deftest is testing]]
            [event-bus :as bus]
            [malli.core :as m])
  (:import [java.io File]
           [java.util UUID]
           [java.util.concurrent CountDownLatch TimeUnit]))

(defn await-on-latch [^CountDownLatch latch]
  (.await latch 1 TimeUnit/SECONDS))

(def any-schema (m/schema :any))

(def test-registry
  {:test/event {"1.0" any-schema}
   :multi/event {"1.0" any-schema}
   :event/inc {"1.0" any-schema}
   :event/child {"1.0" any-schema}
   :event/parent {"1.0" any-schema}
   :event/a {"1.0" any-schema}
   :event/b {"1.0" any-schema}
   :event-1 {"1.0" any-schema}
   :event-2 {"1.0" any-schema}
   :event-3 {"1.0" any-schema}
   :event-4 {"1.0" any-schema}
   :log/break {"1.0" any-schema}
   :log/event {"1.0" (m/schema [:map [:x :int]])}
   :schema/event {"1.0" (m/schema [:map [:x :int]])}
   :event {"1.0" any-schema}
   :partial/event {"1.0" any-schema}
   :c1 {"1.0" any-schema}
   :c2 {"1.0" any-schema}
   :c3 {"1.0" any-schema}
   :any/event {"1.0" any-schema}
   :versioned/event {"1.0" (m/schema [:map [:v :int]])
                     "2.0" (m/schema [:map [:v :string]])}})

(defn make-test-bus [& {:as opts}]
  (apply bus/make-bus (apply concat (merge {:schema-registry test-registry} opts))))

(defn- make-sqlite-config []
  (let [^File file (File/createTempFile "event-bus-" ".db")]
    (.deleteOnExit file)
    {:jdbc-url (str "jdbc:sqlite:" (.getAbsolutePath file))}))

(defn make-tx-store
  ([] (make-tx-store :datahike))
  ([backend]
   (case backend
     :datahike {:db/type :datahike
                :datahike/config {:store {:backend :mem
                                          :id (str (UUID/randomUUID))}
                                  :schema-flexibility :write}}
     :sqlite {:db/type :sqlite
              :sqlite/config (make-sqlite-config)}
     (throw (IllegalArgumentException.
              (str "Unsupported backend in tests: " backend))))))

(defn- run-with-timeout!
  [timeout-ms f]
  (let [result (deref (future (f)) timeout-ms ::timeout)]
    (when (= result ::timeout)
      (is false (str "SQLite test timed out after " timeout-ms " ms")))))

(defn- sqlite-logger []
  (fn [_level data]
    (when (= :tx-worker-failed (:event data))
      (println "SQLite tx-worker failed:" data))))

;; =============================================================================
;; Phase 1: Adapted Tests
;; =============================================================================

(deftest basic-publication-and-subscription-test
  (let [bus (make-test-bus)
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
    (is (await-on-latch latch) "Handler should have been called")
    (is (= :test/event (:message-type @received)))
    (is (= {:data 42} (:payload @received)))
    (bus/close bus)))

(deftest multiple-subscribers-test
  (let [bus (make-test-bus)
        latch (CountDownLatch. 3)
        results (atom [])]
    (dotimes [_ 3]
      (bus/subscribe bus :multi/event
                     (fn [_bus envelope]
                       (swap! results conj (:payload envelope))
                       (.countDown latch))))
    (bus/publish bus :multi/event 1 {:module :test/multi})
    (is (await-on-latch latch) "All 3 handlers should have been called")
    (is (= [1 1 1] (sort @results)))
    (bus/close bus)))

(deftest unsubscribe-test
  (let [bus (make-test-bus)
        latch (CountDownLatch. 1)
        calls (atom 0)
        handler-to-remove (fn [_bus _] (swap! calls inc))]
    ;; Subscribe two handlers
    (bus/subscribe bus :event/inc handler-to-remove)
    (bus/subscribe bus :event/inc (fn [_bus _] (swap! calls inc) (.countDown latch)) {:meta {:id :b}})

    (bus/publish bus :event/inc nil {:module :test/unsub})
    (is (await-on-latch latch))
    (is (= 2 @calls))

    ;; Unsubscribe by handler reference
    (bus/unsubscribe bus :event/inc handler-to-remove)
    (let [latch2 (CountDownLatch. 1)]
      (bus/subscribe bus :event/inc (fn [_bus _] (.countDown latch2)) {:meta {:id :b}}) ; re-add to countdown
      (bus/publish bus :event/inc nil {:module :test/unsub})
      (is (await-on-latch latch2))
      (is (= 3 @calls))) ; 2 from first publish, 1 from second

    ;; Unsubscribe by metadata
    (bus/unsubscribe bus :event/inc {:id :b})
    (bus/publish bus :event/inc nil {:module :test/unsub})
    (Thread/sleep 100) ; Give time for any unexpected calls
    (is (= 3 @calls)) ; Should not have increased
    (bus/close bus)))

(deftest administrative-functions-test
  (let [bus (make-test-bus)]
    (bus/subscribe bus :e1 identity)
    (bus/subscribe bus :e1 identity)
    (bus/subscribe bus :e2 identity)
    (is (= 2 (bus/listener-count bus :e1)))
    (is (= 3 (bus/listener-count bus)))
    (bus/clear-listeners bus :e1)
    (is (= 0 (bus/listener-count bus :e1)))
    (is (= 1 (bus/listener-count bus)))
    (bus/clear-listeners bus)
    (is (= 0 (bus/listener-count bus)))
    (bus/close bus)
    (is (thrown? IllegalStateException (bus/publish bus :any/event nil)))))

;; =============================================================================
;; Phase 2: New Feature Tests
;; =============================================================================

(deftest event-derivation-and-causality-test
  (let [bus (make-test-bus)
        latch (CountDownLatch. 1)
        child-event (atom nil)
        parent-corr-id (atom nil)]
    (bus/subscribe bus :event/child
                   (fn [_bus envelope]
                     (reset! child-event envelope)
                     (.countDown latch)))
    (bus/subscribe bus :event/parent
                   (fn [b envelope]
                     (reset! parent-corr-id (:correlation-id envelope))
                     (bus/publish b :event/child {:child-data 1} {:parent-envelope envelope :module :m/child})))
    (bus/publish bus :event/parent {:parent-data 0} {:module :m/parent})
    (is (await-on-latch latch) "Child event handler should have been called")
    (is (some? @child-event))
    (testing "Correlation ID is passed down"
      (is (and (some? @parent-corr-id)
               (= @parent-corr-id (:correlation-id @child-event)))))
    (testing "Causation path is correctly populated"
      (is (= [[:m/parent :event/parent]] (:causation-path @child-event))))
    (bus/close bus)))

(deftest cycle-detection-test
  (let [bus (make-test-bus :max-depth 2)
        latch (CountDownLatch. 1)
        exception-atom (atom nil)]
    (bus/subscribe bus :event/a
                   (fn [b envelope]
                     ;; Publish B, which will in turn publish A and cause the cycle
                     (bus/publish b :event/b {:b 1} {:parent-envelope envelope :module :m/loop})))
    (bus/subscribe bus :event/b
                   (fn [b envelope]
                     (try
                       (bus/publish b :event/a {:a 2} {:parent-envelope envelope :module :m/loop})
                       (catch IllegalStateException e
                         (reset! exception-atom e))
                       (finally
                         (.countDown latch)))))

    (let [root-envelope (bus/publish bus :event/a {:a 1} {:module :m/loop})]
      (is (await-on-latch latch) "Cycle detection should have triggered")
      (is (instance? IllegalStateException @exception-atom) "An IllegalStateException should have been caught")
      (is (clojure.string/starts-with? (.getMessage @exception-atom) "Cycle detected"))

      ;; Also test that the returned envelope is correct
      (is (= :event/a (:message-type root-envelope)))
      (is (some? (:correlation-id root-envelope))))
    (bus/close bus)))

(deftest max-depth-test
  (let [bus (make-test-bus :max-depth 2)
        latch (CountDownLatch. 1)]
    (bus/subscribe bus :event-1 (fn [b env] (bus/publish b :event-2 2 {:parent-envelope env :module :m/depth})))
    (bus/subscribe bus :event-2 (fn [b env] (bus/publish b :event-3 3 {:parent-envelope env :module :m/depth})))
    (bus/subscribe bus :event-3 (fn [b env]
                                  (is (thrown? IllegalStateException
                                               (bus/publish b :event-4 4 {:parent-envelope env :module :m/depth})))
                                  (.countDown latch)))
    (bus/publish bus :event-1 1 {:module :m/depth})
    (is (await-on-latch latch) "Max depth should have been reached")
    (bus/close bus)))

(deftest handler-error-isolation-test
  (let [bus (make-test-bus)
        latch (CountDownLatch. 1)
        good-handler-called (atom false)]
    (bus/subscribe bus :test/event (fn [_ _] (throw (RuntimeException. "FAIL"))))
    (bus/subscribe bus :test/event (fn [_ _] (reset! good-handler-called true) (.countDown latch)))

    (bus/publish bus :test/event nil {:module :test/handler})
    (is (await-on-latch latch) "Good handler should run despite the other failing")
    (is (true? @good-handler-called))
    (bus/close bus)))

(deftest logger-exception-breaks-publish-test
  (let [bus (make-test-bus :logger (fn [_level data]
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
        bus (make-test-bus :logger (fn [level data] (swap! log-atom conj (assoc data :level level))))
        latch (CountDownLatch. 1)
        good-schema (m/schema [:map [:x :int]])]

    (bus/subscribe bus :log/event (fn [_ _] (throw (RuntimeException. "FAIL"))) {:schema good-schema})
    (bus/subscribe bus :log/event (fn [_ _] (.countDown latch)) {:schema good-schema})
    (bus/subscribe bus :log/event (fn [_ _] (.countDown latch)) {:schema (m/schema [:map [:y :string]])})

    (bus/publish bus :log/event {:x 42} {:module :test/log}) ; PAYLOAD IS NOW VALID FOR THE FIRST TWO
    (is (await-on-latch latch) "Handlers should complete")
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

(deftest publish-schema-validation-test
  (let [log-atom (atom [])
        bus (make-test-bus :logger (fn [level data] (swap! log-atom conj (assoc data :level level))))
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
  (let [bus (make-test-bus :schema-registry {})]
    (is (thrown? IllegalStateException
                 (bus/publish bus :missing/event {} {:module :test/missing})))
    (bus/close bus)))

(deftest make-bus-requires-schema-registry-test
  (is (thrown? IllegalArgumentException
               (bus/make-bus))))

(deftest publish-schema-version-selection-test
  (let [bus (make-test-bus)]
    (let [env-v1 (bus/publish bus :versioned/event {:v 1} {:module :test/version})]
      (is (= "1.0" (:schema-version env-v1))))
    (let [env-v2 (bus/publish bus :versioned/event {:v "ok"} {:module :test/version :schema-version "2.0"})]
      (is (= "2.0" (:schema-version env-v2))))
    (is (thrown? IllegalStateException
                 (bus/publish bus :versioned/event {:v 2} {:module :test/version :schema-version "2.0"})))
    (bus/close bus)))

(deftest buffered-mode-backpressure-test
  (let [bus (make-test-bus :mode :buffered :buffer-size 1 :concurrency 1)
        handler-latch (CountDownLatch. 1)
        release-latch (CountDownLatch. 1)]
    ;; This handler will block, consuming the only worker thread
    (bus/subscribe bus :event
                   (fn [_ _]
                     (.countDown handler-latch)
                     (await-on-latch release-latch)))

    (bus/publish bus :event 1 {:module :test/buffer}) ; Consumes the worker
    (is (await-on-latch handler-latch) "Handler should have started")

    (bus/publish bus :event 2 {:module :test/buffer}) ; Fills the buffer of size 1

    (testing "Publishing to a full buffer throws exception"
      (is (thrown? IllegalStateException (bus/publish bus :event 3 {:module :test/buffer}))))

    (.countDown release-latch) ; Unblock the handler
    (bus/close bus)))

(deftest buffered-partial-delivery-test
  (let [bus (make-test-bus :mode :buffered :buffer-size 1 :concurrency 0)
        called (atom 0)]
    (bus/subscribe bus :partial/event (fn [_ _] (swap! called inc)))
    (bus/subscribe bus :partial/event (fn [_ _] (swap! called inc)))

    (is (thrown? IllegalStateException (bus/publish bus :partial/event nil {:module :test/partial})))

    (let [queue (:queue bus)
          task (.take queue)]
      (task))

    (is (= 1 @called) "One handler should have been enqueued and executed")
    (bus/close bus)))

(deftest causation-path-is-vector-test
  (let [bus (make-test-bus)
        latch (CountDownLatch. 1)
        child-event (atom nil)]
    (bus/subscribe bus :c3
                   (fn [_ env]
                     (reset! child-event env)
                     (.countDown latch)))
    (bus/subscribe bus :c2 (fn [b env] (bus/publish b :c3 3 {:parent-envelope env :module :m/c3})))
    (bus/subscribe bus :c1 (fn [b env] (bus/publish b :c2 2 {:parent-envelope env :module :m/c2})))
    (bus/publish bus :c1 1 {:module :m/c1})
    (is (await-on-latch latch))
    (is (vector? (:causation-path @child-event)))
    (is (= [[:m/c1 :c1] [:m/c2 :c2]] (:causation-path @child-event)))
    (bus/close bus)))


(deftest transact-success-test
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
    (bus/close bus)))

(deftest transact-handler-failure-test
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
    (bus/close bus)))

(deftest transact-timeout-test
  (let [bus (make-test-bus :tx-store (make-tx-store)
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
    (bus/close bus)))

(deftest transact-retry-success-test
  (let [bus (make-test-bus :tx-store (make-tx-store)
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
    (bus/close bus)))

(deftest transact-success-sqlite-test
  (run-with-timeout! 120000
    (fn []
      (let [bus (make-test-bus :tx-store (make-tx-store :sqlite)
                               :logger (sqlite-logger))
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

(deftest transact-handler-failure-sqlite-test
  (run-with-timeout! 120000
    (fn []
      (let [bus (make-test-bus :tx-store (make-tx-store :sqlite)
                               :handler-max-retries 1
                               :logger (sqlite-logger))
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

(deftest transact-timeout-sqlite-test
  (run-with-timeout! 120000
    (fn []
      (let [bus (make-test-bus :tx-store (make-tx-store :sqlite)
                               :tx-handler-timeout 10
                               :handler-max-retries 1
                               :logger (sqlite-logger))
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

(deftest transact-retry-success-sqlite-test
  (run-with-timeout! 120000
    (fn []
      (let [bus (make-test-bus :tx-store (make-tx-store :sqlite)
                               :handler-max-retries 2
                               :handler-backoff-ms 10
                               :logger (sqlite-logger))
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

(comment
  (require '[kaocha.repl :as k])
  (k/run *ns*)
  )
