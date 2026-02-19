(ns event-bus-persistence-datahike
  (:require [clojure.edn :as edn]
            [datahike.api :as d]
            [event-bus-persistence :as p])
  (:import [java.util Date UUID]))

(def ^:private message-schema
  [{:db/ident       :msg/id
    :db/valueType   :db.type/uuid
    :db/unique      :db.unique/identity
    :db/cardinality :db.cardinality/one}
   {:db/ident       :msg/envelope
    :db/valueType   :db.type/string
    :db/cardinality :db.cardinality/one}
   {:db/ident       :msg/status
    :db/valueType   :db.type/keyword
    :db/cardinality :db.cardinality/one}
   {:db/ident       :msg/attempts
    :db/valueType   :db.type/long
    :db/cardinality :db.cardinality/one}
   {:db/ident       :msg/created-at
    :db/valueType   :db.type/instant
    :db/cardinality :db.cardinality/one}
   {:db/ident       :msg/sent-at
    :db/valueType   :db.type/instant
    :db/cardinality :db.cardinality/one}
   {:db/ident       :msg/failed-at
    :db/valueType   :db.type/instant
    :db/cardinality :db.cardinality/one}])

(def ^:private inbox-schema
  [{:db/ident       :inbox/message-id
    :db/valueType   :db.type/uuid
    :db/unique      :db.unique/identity
    :db/cardinality :db.cardinality/one}
   {:db/ident       :inbox/received-at
    :db/valueType   :db.type/instant
    :db/cardinality :db.cardinality/one}])

(def ^:private full-schema
  (into message-schema inbox-schema))

(defn- ensure-db! [config]
  (when-not (d/database-exists? config)
    (d/create-database config))
  (let [conn (d/connect config)]
    (d/transact conn full-schema)
    conn))

(defrecord DatahikeStore [config conn])

(def ^:private default-config
  {:keep-history? false
   :schema-flexibility :read})

(defn- normalize-config [config-or-path]
  (if (string? config-or-path)
    (merge default-config
           {:store {:backend :file
                    :path config-or-path}})
    (merge default-config config-or-path)))

(defn make-store [config-or-path]
  (let [config (normalize-config config-or-path)
        conn (ensure-db! config)]
    (->DatahikeStore config conn)))

(defn close-store! [store]
  (when-let [conn (:conn store)]
    (d/release conn))
  true)

(defn- now [] (Date.))

(defn- envelope->edn [envelope]
  (pr-str envelope))

(defn- edn->envelope [s]
  (edn/read-string {:readers *data-readers*} s))

(defn- entity-id-by-msg-id [conn msg-id]
  (d/q '[:find ?e .
         :in $ ?id
         :where [?e :msg/id ?id]]
       @conn msg-id))

(defn- attempts-by-msg-id [conn msg-id]
  (d/q '[:find ?a .
         :in $ ?id
         :where [?e :msg/id ?id]
                [?e :msg/attempts ?a]]
       @conn msg-id))

(defn- inbox-entity-id [conn msg-id]
  (d/q '[:find ?e .
         :in $ ?id
         :where [?e :inbox/message-id ?id]]
       @conn msg-id))

(extend-type DatahikeStore
  p/PersistenceStore
  (persist-message! [store envelope]
    (let [conn (:conn store)
          msg-id (:message-id envelope)
          tx [{:msg/id msg-id
               :msg/envelope (envelope->edn envelope)
               :msg/status :pending
               :msg/attempts 0
               :msg/created-at (now)}]]
      (d/transact conn tx)
      msg-id))

  (fetch-pending! [store limit]
    (let [conn (:conn store)
          rows (d/q '[:find ?e ?created ?attempts
                      :where [?e :msg/status :pending]
                             [?e :msg/created-at ?created]
                             [?e :msg/attempts ?attempts]]
                    @conn)
          sorted (->> rows
                      (sort-by second)
                      (take limit))]
      (mapv (fn [[e _created attempts]]
              (let [{:msg/keys [id envelope]} (d/pull @conn [:msg/id :msg/envelope] e)]
                {:message-id id
                 :envelope (edn->envelope envelope)
                 :attempts attempts}))
            sorted)))

  (record-attempt! [store message-id]
    (let [conn (:conn store)
          e (entity-id-by-msg-id conn message-id)
          attempts (or (attempts-by-msg-id conn message-id) 0)]
      (when e
        (d/transact conn [[:db/add e :msg/attempts (inc attempts)]]))
      (inc attempts)))

  (mark-sent! [store message-id]
    (let [conn (:conn store)
          e (entity-id-by-msg-id conn message-id)]
      (when e
        (d/transact conn [[:db/add e :msg/status :sent]
                          [:db/add e :msg/sent-at (now)]]))
      true))

  (mark-failed! [store message-id]
    (let [conn (:conn store)
          e (entity-id-by-msg-id conn message-id)]
      (when e
        (d/transact conn [[:db/add e :msg/status :failed]
                          [:db/add e :msg/failed-at (now)]]))
      true))

  (mark-received! [store message-id]
    (let [conn (:conn store)
          existing (inbox-entity-id conn message-id)]
      (when-not existing
        (d/transact conn [{:inbox/message-id message-id
                           :inbox/received-at (now)}]))
      true))

  (was-received? [store message-id]
    (boolean (inbox-entity-id (:conn store) message-id))))
