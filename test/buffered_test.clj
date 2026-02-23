(ns buffered-test
  (:require [clojure.test :refer [deftest is testing]]
            [event-bus :as bus]
            [support :as support])
  (:import [java.util.concurrent CountDownLatch]))

(deftest buffered-mode-backpressure-test
  (let [bus (support/make-test-bus :mode :buffered :buffer-size 1 :concurrency 1)
        handler-latch (CountDownLatch. 1)
        release-latch (CountDownLatch. 1)]
    (bus/subscribe bus :event
                   (fn [_ _]
                     (.countDown handler-latch)
                     (support/await-on-latch release-latch)))

    (bus/publish bus :event 1 {:module :test/buffer})
    (is (support/await-on-latch handler-latch) "Handler should have started")

    (bus/publish bus :event 2 {:module :test/buffer})

    (testing "Publishing to a full buffer throws exception"
      (is (thrown? IllegalStateException (bus/publish bus :event 3 {:module :test/buffer}))))

    (.countDown release-latch)
    (bus/close bus)))

(deftest buffered-partial-delivery-test
  (let [bus (support/make-test-bus :mode :buffered :buffer-size 1 :concurrency 0)
        called (atom 0)]
    (bus/subscribe bus :partial/event (fn [_ _] (swap! called inc)))
    (bus/subscribe bus :partial/event (fn [_ _] (swap! called inc)))

    (is (thrown? IllegalStateException (bus/publish bus :partial/event nil {:module :test/partial})))

    (let [queue (:queue bus)
          task (.take queue)]
      (task))

    (is (= 1 @called) "One handler should have been enqueued and executed")
    (bus/close bus)))
