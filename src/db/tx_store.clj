(ns db.tx-store)

(defprotocol TxStore
  (init! [store])
  (transact! [store tx-data])
  (build-tx-data [store tx-id now events listeners])
  (query-pending-handlers [store now])
  (update-handler! [store update])
  (tx-status [store tx-id])
  (update-tx! [store tx-id status now]))

(defn make-store
  [tx-store]
  (let [db-type (or (:db/type tx-store) :sqlite)]
    (case db-type
      :datahike (do
                  (require 'db.datahike)
                  ((resolve 'db.datahike/make-store) tx-store))
      :sqlite (do
                (require 'db.sqlite)
                ((resolve 'db.sqlite/make-store) tx-store))
      (throw (IllegalArgumentException.
               (str "Unsupported :db/type in :tx-store: " db-type))))))

(defn init-store
  [tx-store]
  (-> tx-store
      make-store
      init!))
