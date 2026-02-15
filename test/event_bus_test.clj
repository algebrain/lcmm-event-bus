(ns event-bus-test
  (:require [clojure.test :refer [deftest is testing]]
            [event-bus :as bus]
            [malli.core :as m])
  (:import [java.util UUID]
           [java.util.concurrent CountDownLatch TimeUnit]))

(defn await-on-latch [^CountDownLatch latch]
  (.await latch 1 TimeUnit/SECONDS))

;; =============================================================================
;; Phase 1: Adapted Tests
;; =============================================================================

(deftest basic-publication-and-subscription-test
  (let [bus (bus/make-bus)
        latch (CountDownLatch. 1)
        received (atom nil)]
    (bus/subscribe bus :test/event
                   (fn [_bus envelope]
                     (reset! received envelope)
                     (.countDown latch)))
    (let [published-envelope (bus/publish bus :test/event {:data 42})]
      (is (some? (:correlation-id published-envelope)) "Published envelope should have a correlation-id")
      (is (instance? UUID (:correlation-id published-envelope)) "correlation-id should be a UUID")
      (is (some? (:message-id published-envelope)) "Published envelope should have a message-id")
      (is (instance? UUID (:message-id published-envelope)) "message-id should be a UUID"))
    (is (await-on-latch latch) "Handler should have been called")
    (is (= :test/event (:message-type @received)))
    (is (= {:data 42} (:payload @received)))
    (bus/close bus)))

(deftest multiple-subscribers-test
  (let [bus (bus/make-bus)
        latch (CountDownLatch. 3)
        results (atom [])]
    (dotimes [_ 3]
      (bus/subscribe bus :multi/event
                     (fn [_bus envelope]
                       (swap! results conj (:payload envelope))
                       (.countDown latch))))
    (bus/publish bus :multi/event 1)
    (is (await-on-latch latch) "All 3 handlers should have been called")
    (is (= [1 1 1] (sort @results)))
    (bus/close bus)))

(deftest unsubscribe-test
  (let [bus (bus/make-bus)
        latch (CountDownLatch. 1)
        calls (atom 0)
        handler-to-remove (fn [_bus _] (swap! calls inc))]
    ;; Subscribe two handlers
    (bus/subscribe bus :event/inc handler-to-remove)
    (bus/subscribe bus :event/inc (fn [_bus _] (swap! calls inc) (.countDown latch)) {:meta {:id :b}})

    (bus/publish bus :event/inc nil)
    (is (await-on-latch latch))
    (is (= 2 @calls))

    ;; Unsubscribe by handler reference
    (bus/unsubscribe bus :event/inc handler-to-remove)
    (let [latch2 (CountDownLatch. 1)]
      (bus/subscribe bus :event/inc (fn [_bus _] (.countDown latch2)) {:meta {:id :b}}) ; re-add to countdown
      (bus/publish bus :event/inc nil)
      (is (await-on-latch latch2))
      (is (= 3 @calls))) ; 2 from first publish, 1 from second

    ;; Unsubscribe by metadata
    (bus/unsubscribe bus :event/inc {:id :b})
    (bus/publish bus :event/inc nil)
    (Thread/sleep 100) ; Give time for any unexpected calls
    (is (= 3 @calls)) ; Should not have increased
    (bus/close bus)))

(deftest administrative-functions-test
  (let [bus (bus/make-bus)]
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
  (let [bus (bus/make-bus)
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
                     (bus/publish b :event/child {:child-data 1} {:parent-envelope envelope})))
    (bus/publish bus :event/parent {:parent-data 0})
    (is (await-on-latch latch) "Child event handler should have been called")
    (is (some? @child-event))
    (testing "Correlation ID is passed down"
      (is (and (some? @parent-corr-id)
               (= @parent-corr-id (:correlation-id @child-event)))))
    (testing "Causation path is correctly populated"
      (is (= #{:event/parent} (:causation-path @child-event))))
    (bus/close bus)))

(deftest cycle-detection-test
  (let [bus (bus/make-bus {:max-depth 2})
        latch (CountDownLatch. 1)
        exception-atom (atom nil)]
    (bus/subscribe bus :event/a
                   (fn [b envelope]
                     ;; Publish B, which will in turn publish A and cause the cycle
                     (bus/publish b :event/b {:b 1} {:parent-envelope envelope})))
    (bus/subscribe bus :event/b
                   (fn [b envelope]
                     (try
                       (bus/publish b :event/a {:a 2} {:parent-envelope envelope})
                       (catch IllegalStateException e
                         (reset! exception-atom e))
                       (finally
                         (.countDown latch)))))

    (let [root-envelope (bus/publish bus :event/a {:a 1})]
      (is (await-on-latch latch) "Cycle detection should have triggered")
      (is (instance? IllegalStateException @exception-atom) "An IllegalStateException should have been caught")
      (is (clojure.string/starts-with? (.getMessage @exception-atom) "Cycle detected"))

      ;; Also test that the returned envelope is correct
      (is (= :event/a (:message-type root-envelope)))
      (is (some? (:correlation-id root-envelope))))
    (bus/close bus)))

(deftest max-depth-test
  (let [bus (bus/make-bus {:max-depth 2})
        latch (CountDownLatch. 1)]
    (bus/subscribe bus :event-1 (fn [b env] (bus/publish b :event-2 2 {:parent-envelope env})))
    (bus/subscribe bus :event-2 (fn [b env] (bus/publish b :event-3 3 {:parent-envelope env})))
    (bus/subscribe bus :event-3 (fn [b env]
                                  (is (thrown? IllegalStateException
                                               (bus/publish b :event-4 4 {:parent-envelope env})))
                                  (.countDown latch)))
    (bus/publish bus :event-1 1)
    (is (await-on-latch latch) "Max depth should have been reached")
    (bus/close bus)))

(deftest handler-error-isolation-test
  (let [bus (bus/make-bus)
        latch (CountDownLatch. 1)
        good-handler-called (atom false)]
    (bus/subscribe bus :test/event (fn [_ _] (throw (RuntimeException. "FAIL"))))
    (bus/subscribe bus :test/event (fn [_ _] (reset! good-handler-called true) (.countDown latch)))

    (bus/publish bus :test/event nil)
    (is (await-on-latch latch) "Good handler should run despite the other failing")
    (is (true? @good-handler-called))
    (bus/close bus)))

(deftest logger-and-schema-validation-test
  (let [log-atom (atom [])
        bus (bus/make-bus {:logger (fn [level data] (swap! log-atom conj (assoc data :level level)))})
        latch (CountDownLatch. 1)
        good-schema (m/schema [:map [:x :int]])]

    (bus/subscribe bus :log/event (fn [_ _] (throw (RuntimeException. "FAIL"))) {:schema good-schema})
    (bus/subscribe bus :log/event (fn [_ _] (.countDown latch)) {:schema good-schema})
    (bus/subscribe bus :log/event (fn [_ _] (.countDown latch)) {:schema (m/schema [:map [:y :string]])})

    (bus/publish bus :log/event {:x 42}) ; PAYLOAD IS NOW VALID FOR THE FIRST TWO
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

(deftest buffered-mode-backpressure-test
  (let [bus (bus/make-bus {:mode :buffered :buffer-size 1 :concurrency 1})
        handler-latch (CountDownLatch. 1)
        release-latch (CountDownLatch. 1)]
    ;; This handler will block, consuming the only worker thread
    (bus/subscribe bus :event
                   (fn [_ _]
                     (.countDown handler-latch)
                     (await-on-latch release-latch)))

    (bus/publish bus :event 1) ; Consumes the worker
    (is (await-on-latch handler-latch) "Handler should have started")

    (bus/publish bus :event 2) ; Fills the buffer of size 1

    (testing "Publishing to a full buffer throws exception"
      (is (thrown? IllegalStateException (bus/publish bus :event 3))))

    (.countDown release-latch) ; Unblock the handler
    (bus/close bus)))


(comment
  (require '[kaocha.repl :as k])
  (k/run *ns*)
  )
