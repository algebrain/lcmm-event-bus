(ns causality-test
  (:require [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [event-bus :as bus]
            [support :as support])
  (:import [java.util.concurrent CountDownLatch]))

(deftest event-derivation-and-causality-test
  (let [bus (support/make-test-bus)
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
    (is (support/await-on-latch latch) "Child event handler should have been called")
    (is (some? @child-event))
    (testing "Correlation ID is passed down"
      (is (and (some? @parent-corr-id)
               (= @parent-corr-id (:correlation-id @child-event)))))
    (testing "Causation path is correctly populated"
      (is (= [[:m/parent :event/parent]] (:causation-path @child-event))))
    (bus/close bus)))

(deftest cycle-detection-test
  (let [bus (support/make-test-bus :max-depth 2)
        latch (CountDownLatch. 1)
        exception-atom (atom nil)]
    (bus/subscribe bus :event/a
                   (fn [b envelope]
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
      (is (support/await-on-latch latch) "Cycle detection should have triggered")
      (is (instance? IllegalStateException @exception-atom) "An IllegalStateException should have been caught")
      (is (str/starts-with? (.getMessage @exception-atom) "Cycle detected"))
      (is (= :event/a (:event-type root-envelope)))
      (is (some? (:correlation-id root-envelope))))
    (bus/close bus)))

(deftest max-depth-test
  (let [bus (support/make-test-bus :max-depth 2)
        latch (CountDownLatch. 1)]
    (bus/subscribe bus :event-1 (fn [b env] (bus/publish b :event-2 2 {:parent-envelope env :module :m/depth})))
    (bus/subscribe bus :event-2 (fn [b env] (bus/publish b :event-3 3 {:parent-envelope env :module :m/depth})))
    (bus/subscribe bus :event-3 (fn [b env]
                                  (is (thrown? IllegalStateException
                                               (bus/publish b :event-4 4 {:parent-envelope env :module :m/depth})))
                                  (.countDown latch)))
    (bus/publish bus :event-1 1 {:module :m/depth})
    (is (support/await-on-latch latch) "Max depth should have been reached")
    (bus/close bus)))

(deftest causation-path-is-vector-test
  (let [bus (support/make-test-bus)
        latch (CountDownLatch. 1)
        child-event (atom nil)]
    (bus/subscribe bus :c3
                   (fn [_ env]
                     (reset! child-event env)
                     (.countDown latch)))
    (bus/subscribe bus :c2 (fn [b env] (bus/publish b :c3 3 {:parent-envelope env :module :m/c3})))
    (bus/subscribe bus :c1 (fn [b env] (bus/publish b :c2 2 {:parent-envelope env :module :m/c2})))
    (bus/publish bus :c1 1 {:module :m/c1})
    (is (support/await-on-latch latch))
    (is (vector? (:causation-path @child-event)))
    (is (= [[:m/c1 :c1] [:m/c2 :c2]] (:causation-path @child-event)))
    (bus/close bus)))
