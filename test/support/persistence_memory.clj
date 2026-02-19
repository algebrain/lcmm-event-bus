(ns support.persistence-memory
  (:require [event-bus-persistence :as persistence]))

(defn memory-store []
  (let [state (atom {:messages {}
                     :inbox #{}})]
    {:state state
     :store (reify persistence/PersistenceStore
              (persist-message! [_ envelope]
                (swap! state update :messages assoc
                       (:message-id envelope)
                       {:envelope envelope
                        :status :pending
                        :attempts 0
                        :created-at (System/currentTimeMillis)})
                (:message-id envelope))
              (fetch-pending! [_ limit]
                (->> (:messages @state)
                     (vals)
                     (filter #(= :pending (:status %)))
                     (sort-by :created-at)
                     (take limit)
                     (mapv (fn [m]
                             {:message-id (:message-id (:envelope m))
                              :envelope (:envelope m)
                              :attempts (:attempts m)}))))
              (record-attempt! [_ message-id]
                (swap! state update-in [:messages message-id :attempts] (fnil inc 0))
                (get-in @state [:messages message-id :attempts]))
              (mark-sent! [_ message-id]
                (swap! state assoc-in [:messages message-id :status] :sent)
                true)
              (mark-failed! [_ message-id]
                (swap! state assoc-in [:messages message-id :status] :failed)
                true)
              (mark-received! [_ message-id]
                (swap! state update :inbox conj message-id)
                true)
              (was-received? [_ message-id]
                (contains? (:inbox @state) message-id)))}))

(defn status-by-id [state msg-id]
  (get-in @state [:messages msg-id :status]))
