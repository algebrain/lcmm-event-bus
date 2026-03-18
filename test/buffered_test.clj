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

(deftest buffered-publish-is-atomic-at-enqueue-boundary-test
  (let [bus (support/make-test-bus :mode :buffered :buffer-size 1 :concurrency 0)
        received (atom [])]
    (bus/subscribe bus :partial/event (fn [_ envelope] (swap! received conj (:payload envelope))))
    (bus/subscribe bus :partial/event (fn [_ envelope] (swap! received conj (:payload envelope))))

    (bus/publish bus :partial/event {:id 1} {:module :test/partial})
    (is (thrown? IllegalStateException
                 (bus/publish bus :partial/event {:id 2} {:module :test/partial})))
    (is (= 1 (.size (:queue bus))) "Only the accepted event should occupy the queue")

    (let [item (.take (:queue bus))]
      (is (= :dispatch-event (:kind item)))
      (is (= {:id 1} (-> item :envelope :payload)))
      (#'event-bus/dispatch-queued-item! bus item))

    (is (= [{:id 1} {:id 1}] @received)
        "Rejected publish must not leak partial delivery to listeners")
    (is (zero? (.size (:queue bus))) "The queue should drain only accepted events")
    (bus/close bus)))

(deftest buffered-queue-stores-dispatch-items-test
  (let [bus (support/make-test-bus :mode :buffered :buffer-size 2 :concurrency 0)]
    (bus/subscribe bus :event (fn [_ _] nil))

    (bus/publish bus :event {:data 1} {:module :test/buffer})

    (let [queued-item (.take (:queue bus))]
      (is (map? queued-item) "Buffered queue should contain a dispatch item")
      (is (= :dispatch-event (:kind queued-item)))
      (is (= :event (-> queued-item :envelope :event-type)))
      (is (= {:data 1} (-> queued-item :envelope :payload))))
    (bus/close bus)))

(deftest buffered-one-publish-occupies-one-queue-slot-test
  (let [bus (support/make-test-bus :mode :buffered :buffer-size 4 :concurrency 0)
        called (atom 0)]
    (dotimes [_ 3]
      (bus/subscribe bus :partial/event (fn [_ _] (swap! called inc))))

    (bus/publish bus :partial/event nil {:module :test/fanout})

    (is (= 1 (.size (:queue bus))) "One publish should occupy one queue slot regardless of listener count")

    (let [item (.take (:queue bus))]
      (#'event-bus/dispatch-queued-item! bus item))

    (is (= 3 @called) "Dispatching one queued event should call all listeners")
    (bus/close bus)))

(deftest buffered-fanout-happens-after-dequeue-test
  (let [bus (support/make-test-bus :mode :buffered :buffer-size 1 :concurrency 0)
        called (atom 0)]
    (bus/subscribe bus :partial/event (fn [_ _] (swap! called inc)))
    (bus/subscribe bus :partial/event (fn [_ _] (swap! called inc)))

    (bus/publish bus :partial/event nil {:module :test/fanout})
    (is (= 1 (.size (:queue bus))) "Fanout should not multiply queue pressure before dequeue")

    (let [item (.take (:queue bus))]
      (#'event-bus/dispatch-queued-item! bus item))

    (is (= 2 @called) "All listeners should run after one dequeue")
    (bus/close bus)))

(deftest buffered-slow-handler-saturates-quickly-test
  (let [bus (support/make-test-bus :mode :buffered :buffer-size 2 :concurrency 1)
        started (CountDownLatch. 1)
        release (CountDownLatch. 1)
        processed (atom 0)]
    (dotimes [_ 3]
      (bus/subscribe bus :event
                     (fn [_ _]
                       (.countDown started)
                       (support/await-on-latch release)
                       (swap! processed inc))))

    (bus/publish bus :event 0 {:module :test/slow})
    (is (support/await-on-latch started) "The first handler should be running before the burst")

    (let [results (doall
                   (for [n (range 1 10)]
                     (try
                       (bus/publish bus :event n {:module :test/slow})
                       :accepted
                       (catch IllegalStateException _
                         :rejected))))
          accepted (count (filter #{:accepted} results))
          rejected (count (filter #{:rejected} results))]
      (is (= 2 accepted) "Only the remaining queue capacity should accept extra events")
      (is (= 7 rejected) "Once the queue is full the rest of the burst should fail fast despite fanout"))

    (.countDown release)
    (is (support/wait-until 1000 #(= 9 @processed))
        "Accepted events should fan out after dequeue without consuming extra queue slots")
    (bus/close bus)))
