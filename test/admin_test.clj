(ns admin-test
  (:require [clojure.test :refer [deftest is]]
            [event-bus :as bus]
            [support :as support])
  (:import [java.util.concurrent CountDownLatch]))

(deftest unsubscribe-test
  (let [bus (support/make-test-bus)
        latch (CountDownLatch. 1)
        calls (atom 0)
        handler-to-remove (fn [_bus _] (swap! calls inc))]
    (bus/subscribe bus :event/inc handler-to-remove)
    (bus/subscribe bus :event/inc (fn [_bus _] (swap! calls inc) (.countDown latch)) {:meta {:id :b}})

    (bus/publish bus :event/inc nil {:module :test/unsub})
    (is (support/await-on-latch latch))
    (is (= 2 @calls))

    (bus/unsubscribe bus :event/inc handler-to-remove)
    (let [latch2 (CountDownLatch. 1)]
      (bus/subscribe bus :event/inc (fn [_bus _] (.countDown latch2)) {:meta {:id :b}})
      (bus/publish bus :event/inc nil {:module :test/unsub})
      (is (support/await-on-latch latch2))
      (is (= 3 @calls)))

    (bus/unsubscribe bus :event/inc {:id :b})
    (bus/publish bus :event/inc nil {:module :test/unsub})
    (Thread/sleep 100)
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
