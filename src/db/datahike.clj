(ns db.datahike
  (:require [datahike.api :as d]
            [db.tx-store :as tx-store])
  (:import [java.util UUID]))

(def ^:private tx-schema
  [{:db/ident :tx/id
    :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :tx/status
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one}
   {:db/ident :tx/created-at
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one}
   {:db/ident :tx/updated-at
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one}
   {:db/ident :msg/id
    :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :msg/tx
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident :msg/event-type
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one}
   {:db/ident :msg/payload
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :msg/module
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one}
   {:db/ident :msg/schema-version
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :msg/correlation-id
    :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/one}
   {:db/ident :msg/message-id
    :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/one}
   {:db/ident :h/id
    :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity}
   {:db/ident :h/msg
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one}
   {:db/ident :h/handler-id
    :db/valueType :db.type/uuid
    :db/cardinality :db.cardinality/one}
   {:db/ident :h/status
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one}
   {:db/ident :h/retry-count
    :db/valueType :db.type/long
    :db/cardinality :db.cardinality/one}
   {:db/ident :h/last-error
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident :h/updated-at
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one}
   {:db/ident :h/next-at
    :db/valueType :db.type/instant
    :db/cardinality :db.cardinality/one}])

(defrecord DatahikeStore [config conn])

(defn make-store
  [{:keys [datahike/config]}]
  (when-not config
    (throw (IllegalArgumentException. "Missing :datahike/config in :tx-store.")))
  (->DatahikeStore config nil))

(extend-type DatahikeStore
  tx-store/TxStore
  (init! [store]
    (try
      (d/create-database (:config store))
      (catch Throwable _ nil))
    (let [conn (d/connect (:config store))]
      (d/transact conn tx-schema)
      (assoc store :conn conn)))

  (transact! [store tx-data]
    (d/transact (:conn store) tx-data))

  (build-tx-data [store tx-id now events listeners]
    (let [tx-eid (d/tempid :db.part/user)
          base-tx {:db/id tx-eid
                   :tx/id tx-id
                   :tx/status :pending
                   :tx/created-at now
                   :tx/updated-at now}]
      (reduce
        (fn [{:keys [tx-data handler-count]} {:keys [event-type payload module schema-version]}]
          (let [schema-version (or schema-version "1.0")
                msg-eid (d/tempid :db.part/user)
                msg-id (UUID/randomUUID)
                message-id (UUID/randomUUID)
                msg {:db/id msg-eid
                     :msg/id msg-id
                     :msg/tx tx-eid
                     :msg/event-type event-type
                     :msg/payload (pr-str payload)
                     :msg/module module
                     :msg/schema-version schema-version
                     :msg/correlation-id tx-id
                     :msg/message-id message-id}
                handlers (get listeners event-type [])
                handler-entities (mapv (fn [{:keys [id]}]
                                         {:db/id (d/tempid :db.part/user)
                                          :h/id (UUID/randomUUID)
                                          :h/msg msg-eid
                                          :h/handler-id id
                                          :h/status :pending
                                          :h/retry-count 0
                                          :h/updated-at now
                                          :h/next-at now})
                                       handlers)]
            {:tx-data (into tx-data (cons msg handler-entities))
             :handler-count (+ handler-count (count handler-entities))}))
        {:tx-data [base-tx]
         :handler-count 0}
        events)))

  (query-pending-handlers [store now]
    (let [db (d/db (:conn store))]
      (d/q '[:find ?h ?msg ?tx-id ?event-type ?payload ?module ?schema-version
                     ?correlation-id ?message-id ?handler-id ?retry-count
              :in $ ?now
              :where
              [?h :h/status :pending]
              [?h :h/next-at ?next-at]
              [(<= ?next-at ?now)]
              [?h :h/msg ?msg]
              [?h :h/handler-id ?handler-id]
              [?h :h/retry-count ?retry-count]
              [?msg :msg/tx ?tx]
              [?tx :tx/id ?tx-id]
              [?msg :msg/event-type ?event-type]
              [?msg :msg/payload ?payload]
              [?msg :msg/module ?module]
              [?msg :msg/schema-version ?schema-version]
              [?msg :msg/correlation-id ?correlation-id]
              [?msg :msg/message-id ?message-id]]
            db now)))

  (update-handler! [store update]
    (let [entity {:db/id (:handler-row-id update)
                  :h/status (:status update)
                  :h/retry-count (:retry-count update)
                  :h/updated-at (:updated-at update)
                  :h/next-at (:next-at update)}]
      (d/transact (:conn store)
                  [(cond-> entity
                     (:last-error update) (assoc :h/last-error (:last-error update)))])))

  (tx-status [store tx-id]
    (let [db (d/db (:conn store))
          statuses (map first
                        (d/q '[:find ?status
                               :in $ ?tx-id
                               :where
                               [?tx :tx/id ?tx-id]
                               [?msg :msg/tx ?tx]
                               [?h :h/msg ?msg]
                               [?h :h/status ?status]]
                             db tx-id))]
      (cond
        (empty? statuses) :ok
        (some #{:failed :timeout} statuses) :failed
        (every? #{:ok} statuses) :ok
        :else :pending)))

  (update-tx! [store tx-id status now]
    (d/transact (:conn store)
                [{:tx/id tx-id
                  :tx/status status
                  :tx/updated-at now}])))
