(ns admin-test
  (:require [clojure.test :refer [deftest is]]
            [event-bus :as bus]
            [support :as support])
  (:import [java.util.concurrent CountDownLatch]))

(deftest unsubscribe-test
  (let [bus (support/make-test-bus)
        calls (atom 0)
        phase-latch (atom nil)
        signal! (fn [] (when-let [l @phase-latch] (.countDown ^CountDownLatch l)))
        handler-to-remove (fn [_bus _] (swap! calls inc) (signal!))
        meta-handler (fn [_bus _] (swap! calls inc) (signal!))]
    (bus/subscribe bus :event/inc handler-to-remove)
    (bus/subscribe bus :event/inc meta-handler {:meta {:id :b}})

    (reset! phase-latch (CountDownLatch. 2))
    (bus/publish bus :event/inc nil {:module :test/unsub})
    (is (support/await-on-latch @phase-latch))
    (is (= 2 @calls))

    (bus/unsubscribe bus :event/inc handler-to-remove)
    (reset! phase-latch (CountDownLatch. 1))
    (bus/publish bus :event/inc nil {:module :test/unsub})
    (is (support/await-on-latch @phase-latch))
    (is (= 3 @calls))

    (bus/unsubscribe bus :event/inc {:id :b})
    (reset! phase-latch (CountDownLatch. 1))
    (bus/publish bus :event/inc nil {:module :test/unsub})
    (is (false? (support/await-on-latch @phase-latch)))
    (is (= 3 @calls))
    (bus/close bus)))

(deftest administrative-functions-test
  (let [bus (support/make-test-bus)]
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
