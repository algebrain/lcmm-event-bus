(ns db.sqlite
  (:require [clojure.edn :as edn]
            [db.tx-store :as tx-store])
  (:import [java.sql DriverManager PreparedStatement ResultSet]
           [java.util UUID]
           [java.util.concurrent.locks ReentrantLock]))

(def ^:private default-pragma
  {:journal_mode "WAL"
   :synchronous "NORMAL"
   :foreign_keys "ON"
   :temp_store "MEMORY"})

(def ^:private ddl-statements
  [(str "CREATE TABLE IF NOT EXISTS tx ("
        "  tx_id TEXT PRIMARY KEY,"
        "  status TEXT NOT NULL,"
        "  created_at INTEGER NOT NULL,"
        "  updated_at INTEGER NOT NULL"
        ");")
   (str "CREATE TABLE IF NOT EXISTS msg ("
        "  msg_id TEXT PRIMARY KEY,"
        "  tx_id TEXT NOT NULL,"
        "  event_type TEXT NOT NULL,"
        "  payload TEXT NOT NULL,"
        "  module TEXT NOT NULL,"
        "  schema_version TEXT NOT NULL,"
        "  correlation_id TEXT NOT NULL,"
        "  message_id TEXT NOT NULL,"
        "  FOREIGN KEY (tx_id) REFERENCES tx(tx_id) ON DELETE CASCADE"
        ");")
   (str "CREATE TABLE IF NOT EXISTS handler ("
        "  h_id TEXT PRIMARY KEY,"
        "  msg_id TEXT NOT NULL,"
        "  handler_id TEXT NOT NULL,"
        "  status TEXT NOT NULL,"
        "  retry_count INTEGER NOT NULL,"
        "  last_error TEXT,"
        "  updated_at INTEGER NOT NULL,"
        "  next_at INTEGER NOT NULL,"
        "  FOREIGN KEY (msg_id) REFERENCES msg(msg_id) ON DELETE CASCADE"
        ");")
   "CREATE INDEX IF NOT EXISTS idx_handler_status_next ON handler(status, next_at);"
   "CREATE INDEX IF NOT EXISTS idx_msg_tx ON msg(tx_id);"])

(defrecord SQLiteStore [jdbc-url conn lock payload-format pragma])

(defn- kw->str [k]
  (when k (pr-str k)))

(defn- str->kw [s]
  (when s (edn/read-string s)))

(defn- uuid->str [^UUID u]
  (when u (.toString u)))

(defn- str->uuid [^String s]
  (when s (UUID/fromString s)))

(defn- now->millis [now]
  (.getTime ^java.util.Date now))

(defn- time->millis [value]
  (cond
    (instance? java.util.Date value) (.getTime ^java.util.Date value)
    (number? value) (long value)
    :else (throw (IllegalArgumentException.
                   (str "Unsupported time value: " (pr-str value))))))

(defn- pragma-value [v]
  (cond
    (true? v) "ON"
    (false? v) "OFF"
    (keyword? v) (name v)
    :else (str v)))

(defn- apply-pragmas!
  [^java.sql.Connection conn pragma]
  (doseq [[k v] pragma]
    (with-open [stmt (.createStatement conn)]
      (.execute stmt (str "PRAGMA " (name k) "=" (pragma-value v)))))) 

(defn- create-schema!
  [^java.sql.Connection conn]
  (doseq [ddl ddl-statements]
    (with-open [stmt (.createStatement conn)]
      (.execute stmt ddl))))

(defn- encode-payload
  [store payload]
  (let [fmt (:payload-format store)]
    (case fmt
      :edn (pr-str payload)
      :value (pr-str payload)
      (pr-str payload))))

(defn- decode-payload
  [store payload-str]
  (case (:payload-format store)
    :value (edn/read-string payload-str)
    payload-str))

(defn make-store
  [{:keys [sqlite/config]}]
  (when-not config
    (throw (IllegalArgumentException. "Missing :sqlite/config in :tx-store.")))
  (let [jdbc-url (or (:jdbc-url config)
                     (when-let [path (:path config)]
                       (str "jdbc:sqlite:" path)))]
    (when-not jdbc-url
      (throw (IllegalArgumentException. "Missing :jdbc-url (or :path) in :sqlite/config.")))
    (let [payload-format (or (:payload-format config) :edn)
          pragma (merge default-pragma (:pragma config))]
      (when-not (#{:edn :value} payload-format)
        (throw (IllegalArgumentException.
                 (str "Unsupported :payload-format in :sqlite/config: " payload-format))))
      (->SQLiteStore jdbc-url nil (ReentrantLock.) payload-format pragma))))

(extend-type SQLiteStore
  tx-store/TxStore
  (init! [store]
    (let [conn (DriverManager/getConnection (:jdbc-url store))]
      (apply-pragmas! conn (:pragma store))
      (create-schema! conn)
      (assoc store :conn conn)))

  (transact! [store tx-data]
    (let [{:keys [tx msgs handlers]} tx-data
          ^java.sql.Connection conn (:conn store)
          lock ^ReentrantLock (:lock store)]
      (.lock lock)
      (try
        (let [prev-autocommit (.getAutoCommit conn)]
          (.setAutoCommit conn false)
          (try
            (with-open [tx-stmt (.prepareStatement conn
                                                  "INSERT INTO tx (tx_id, status, created_at, updated_at) VALUES (?,?,?,?)")]
              (.setString tx-stmt 1 (uuid->str (:tx/id tx)))
              (.setString tx-stmt 2 (name (:tx/status tx)))
              (.setLong tx-stmt 3 (:tx/created-at tx))
              (.setLong tx-stmt 4 (:tx/updated-at tx))
              (.executeUpdate tx-stmt))
            (with-open [msg-stmt (.prepareStatement conn
                                                   "INSERT INTO msg (msg_id, tx_id, event_type, payload, module, schema_version, correlation_id, message_id) VALUES (?,?,?,?,?,?,?,?)")]
              (doseq [m msgs]
                (.setString msg-stmt 1 (uuid->str (:msg/id m)))
                (.setString msg-stmt 2 (uuid->str (:msg/tx-id m)))
                (.setString msg-stmt 3 (:msg/event-type m))
                (.setString msg-stmt 4 (:msg/payload m))
                (.setString msg-stmt 5 (:msg/module m))
                (.setString msg-stmt 6 (:msg/schema-version m))
                (.setString msg-stmt 7 (uuid->str (:msg/correlation-id m)))
                (.setString msg-stmt 8 (uuid->str (:msg/message-id m)))
                (.addBatch msg-stmt))
              (.executeBatch msg-stmt))
            (with-open [h-stmt (.prepareStatement conn
                                                 "INSERT INTO handler (h_id, msg_id, handler_id, status, retry_count, last_error, updated_at, next_at) VALUES (?,?,?,?,?,?,?,?)")]
              (doseq [h handlers]
                (.setString h-stmt 1 (uuid->str (:h/id h)))
                (.setString h-stmt 2 (uuid->str (:h/msg-id h)))
                (.setString h-stmt 3 (uuid->str (:h/handler-id h)))
                (.setString h-stmt 4 (name (:h/status h)))
                (.setLong h-stmt 5 (:h/retry-count h))
                (.setString h-stmt 6 (:h/last-error h))
                (.setLong h-stmt 7 (:h/updated-at h))
                (.setLong h-stmt 8 (:h/next-at h))
                (.addBatch h-stmt))
              (.executeBatch h-stmt))
            (.commit conn)
            (catch Throwable e
              (.rollback conn)
              (throw e))
            (finally
              (.setAutoCommit conn prev-autocommit))))
        (finally
          (.unlock lock)))))

  (build-tx-data [store tx-id now events listeners]
    (let [now-ms (now->millis now)
          tx {:tx/id tx-id
              :tx/status :pending
              :tx/created-at now-ms
              :tx/updated-at now-ms}]
      (reduce
        (fn [{:keys [tx-data handler-count]} {:keys [event-type payload module schema-version]}]
          (let [schema-version (or schema-version "1.0")
                msg-id (UUID/randomUUID)
                message-id (UUID/randomUUID)
                msg {:msg/id msg-id
                     :msg/tx-id tx-id
                     :msg/event-type (kw->str event-type)
                     :msg/payload (encode-payload store payload)
                     :msg/module (kw->str module)
                     :msg/schema-version schema-version
                     :msg/correlation-id tx-id
                     :msg/message-id message-id}
                handlers (get listeners event-type [])
                handler-entities (mapv (fn [{:keys [id]}]
                                         {:h/id (UUID/randomUUID)
                                          :h/msg-id msg-id
                                          :h/handler-id id
                                          :h/status :pending
                                          :h/retry-count 0
                                          :h/updated-at now-ms
                                          :h/next-at now-ms
                                          :h/last-error nil})
                                       handlers)]
            {:tx-data (-> tx-data
                          (update :msgs conj msg)
                          (update :handlers into handler-entities))
             :handler-count (+ handler-count (count handler-entities))}))
        {:tx-data {:tx tx :msgs [] :handlers []}
         :handler-count 0}
        events)))

  (query-pending-handlers [store now]
    (let [conn (:conn store)
          lock ^ReentrantLock (:lock store)
          now-ms (now->millis now)
          sql (str "SELECT h.h_id, m.msg_id, t.tx_id, m.event_type, m.payload, m.module, "
                   "m.schema_version, m.correlation_id, m.message_id, h.handler_id, h.retry_count "
                   "FROM handler h "
                   "JOIN msg m ON h.msg_id = m.msg_id "
                   "JOIN tx t ON m.tx_id = t.tx_id "
                   "WHERE h.status = ? AND h.next_at <= ?")]
      (.lock lock)
      (try
        (with-open [stmt (.prepareStatement conn sql)]
          (.setString stmt 1 "pending")
          (.setLong stmt 2 now-ms)
          (with-open [rs (.executeQuery stmt)]
            (loop [rows []]
              (if (.next rs)
                (let [h-id (str->uuid (.getString rs 1))
                      msg-id (str->uuid (.getString rs 2))
                      tx-id (str->uuid (.getString rs 3))
                      event-type (str->kw (.getString rs 4))
                      payload (decode-payload store (.getString rs 5))
                      module (str->kw (.getString rs 6))
                      schema-version (.getString rs 7)
                      correlation-id (str->uuid (.getString rs 8))
                      message-id (str->uuid (.getString rs 9))
                      handler-id (str->uuid (.getString rs 10))
                      retry-count (.getLong rs 11)]
                  (recur (conj rows [h-id msg-id tx-id event-type payload module schema-version
                                     correlation-id message-id handler-id retry-count])))
                rows))))
        (finally
          (.unlock lock)))))

  (update-handler! [store update]
    (let [conn (:conn store)
          lock ^ReentrantLock (:lock store)
          sql "UPDATE handler SET status=?, retry_count=?, last_error=?, updated_at=?, next_at=? WHERE h_id=?"]
      (.lock lock)
      (try
        (with-open [stmt (.prepareStatement conn sql)]
          (.setString stmt 1 (name (:status update)))
          (.setLong stmt 2 (long (:retry-count update)))
          (.setString stmt 3 (:last-error update))
          (.setLong stmt 4 (time->millis (:updated-at update)))
          (.setLong stmt 5 (time->millis (:next-at update)))
          (.setString stmt 6 (uuid->str (:handler-row-id update)))
          (.executeUpdate stmt))
        (finally
          (.unlock lock)))))

  (tx-status [store tx-id]
    (let [conn (:conn store)
          lock ^ReentrantLock (:lock store)
          sql (str "SELECT h.status FROM handler h "
                   "JOIN msg m ON h.msg_id = m.msg_id "
                   "WHERE m.tx_id = ?")]
      (.lock lock)
      (try
        (with-open [stmt (.prepareStatement conn sql)]
          (.setString stmt 1 (uuid->str tx-id))
          (with-open [rs (.executeQuery stmt)]
            (let [statuses (loop [acc []]
                             (if (.next rs)
                               (recur (conj acc (keyword (.getString rs 1))))
                               acc))]
              (cond
                (empty? statuses) :ok
                (some #{:failed :timeout} statuses) :failed
                (every? #{:ok} statuses) :ok
                :else :pending))))
        (finally
          (.unlock lock)))))

  (update-tx! [store tx-id status now]
    (let [conn (:conn store)
          lock ^ReentrantLock (:lock store)
          sql "UPDATE tx SET status=?, updated_at=? WHERE tx_id=?"]
      (.lock lock)
      (try
        (with-open [stmt (.prepareStatement conn sql)]
          (.setString stmt 1 (name status))
          (.setLong stmt 2 (now->millis now))
          (.setString stmt 3 (uuid->str tx-id))
          (.executeUpdate stmt))
        (finally
          (.unlock lock))))))
