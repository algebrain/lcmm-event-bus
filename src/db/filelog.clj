(ns db.filelog
  (:require [clojure.edn :as edn]
            [db.tx-store :as tx-store])
  (:import [java.io BufferedReader BufferedWriter File FileInputStream FileOutputStream
            InputStreamReader OutputStreamWriter]
           [java.util UUID]
           [java.util.concurrent LinkedBlockingQueue ScheduledThreadPoolExecutor TimeUnit]
           [java.util.concurrent.locks ReentrantLock]))

(def ^:private default-sync? true)
(def ^:private default-queue-size 8192)

(defrecord FileLogStore [path writer fd lock state sync-mode fsync-interval-ms
                         flusher-stop flusher-thread pending-promises
                         handler-queue scheduler])

(defn- now->millis [now]
  (.getTime ^java.util.Date now))

(defn- time->millis [value]
  (cond
    (instance? java.util.Date value) (.getTime ^java.util.Date value)
    (number? value) (long value)
    :else (throw (IllegalArgumentException.
                  (str "Unsupported time value: " (pr-str value))))))

(defn- ensure-parent-dir! [^File file]
  (when-let [parent (.getParentFile file)]
    (.mkdirs parent)))

(defn- open-writer
  [^File file]
  (ensure-parent-dir! file)
  (let [fos (FileOutputStream. file true)
        writer (BufferedWriter. (OutputStreamWriter. fos "UTF-8"))]
    {:writer writer :fd (.getFD fos)}))

(defn- encode-payload [payload]
  (pr-str payload))

(defn- empty-state []
  {:txs {}
   :msgs {}
   :handlers {}})

(defn- write-record!
  [store record]
  (let [^BufferedWriter writer (:writer store)
        line (str (pr-str record) "\n")]
    (.write writer line)
    (.flush writer)))

(defn- sync-store! [store]
  (.sync (:fd store)))

(defn- drain-pending!
  [store]
  (let [pending (:pending-promises store)
        batch @pending]
    (when (seq batch)
      (reset! pending [])
      batch)))

(defn- flush-pending!
  [store]
  (let [lock ^ReentrantLock (:lock store)]
    (.lock lock)
    (try
      (let [batch (drain-pending! store)]
        (when (seq batch)
          (sync-store! store)
          (doseq [p batch]
            (deliver p :ok))))
      (finally
        (.unlock lock)))))

(defn- start-flusher!
  [store]
  (if (not= :batched (:sync-mode store))
    store
    (let [stop-flag (atom false)
          interval-ms (long (:fsync-interval-ms store))
          thread (future
                   (loop []
                     (if @stop-flag
                       (flush-pending! store)
                       (do
                         (flush-pending! store)
                         (Thread/sleep interval-ms)
                         (recur)))))]
      (assoc store :flusher-stop stop-flag :flusher-thread thread))))

(defn- apply-tx!
  [state {:keys [tx msgs handlers]}]
  (let [tx-id (:tx/id tx)]
    (-> state
        (assoc-in [:txs tx-id] tx)
        (update :msgs into (map (fn [m] [(:msg/id m) m]) msgs))
        (update :handlers into (map (fn [h] [(:h/id h) h]) handlers)))))

(defn- apply-handler-update!
  [state update]
  (let [hid (:handler-row-id update)]
    (if-let [existing (get-in state [:handlers hid])]
      (assoc-in state [:handlers hid]
                (merge existing
                       {:h/status (:status update)
                        :h/retry-count (:retry-count update)
                        :h/last-error (:last-error update)
                        :h/updated-at (time->millis (:updated-at update))
                        :h/next-at (time->millis (:next-at update))}))
      state)))

(defn- apply-tx-update!
  [state {:keys [tx-id status now]}]
  (if-let [existing (get-in state [:txs tx-id])]
    (assoc-in state [:txs tx-id]
              (merge existing
                     {:tx/status status
                      :tx/updated-at (time->millis now)}))
    state))

(defn- apply-cleanup!
  [state tx-ids]
  (reduce
   (fn [s tx-id]
     (let [msg-ids (->> (:msgs s)
                        (keep (fn [[mid m]]
                                (when (= tx-id (:msg/tx-id m)) mid)))
                        vec)
           handler-ids (->> (:handlers s)
                            (keep (fn [[hid h]]
                                    (when (some #{(:h/msg-id h)} msg-ids) hid)))
                            vec)]
       (-> s
           (update :txs dissoc tx-id)
           (update :msgs #(apply dissoc % msg-ids))
           (update :handlers #(apply dissoc % handler-ids)))))
   state
   tx-ids))

(defn- handler-row
  [state hid]
  (when-let [h (get-in state [:handlers hid])]
    (when-let [msg (get-in state [:msgs (:h/msg-id h)])]
      [hid
       (:msg/id msg)
       (:msg/tx-id msg)
       (:msg/event-type msg)
       (:msg/payload msg)
       (:msg/module msg)
       (:msg/schema-version msg)
       (:msg/correlation-id msg)
       (:msg/message-id msg)
       (:h/handler-id h)
       (:h/retry-count h)])))

(defn- enqueue-handler!
  [store row]
  (when row
    (.offer ^LinkedBlockingQueue (:handler-queue store) row)))

(defn- schedule-handler!
  [store row delay-ms]
  (if (pos? delay-ms)
    (.schedule ^ScheduledThreadPoolExecutor (:scheduler store)
               ^Runnable (fn [] (enqueue-handler! store row))
               (long delay-ms)
               TimeUnit/MILLISECONDS)
    (enqueue-handler! store row)))

(defn- enqueue-pending-from-state!
  [store state]
  (let [now-ms (System/currentTimeMillis)]
    (doseq [[hid h] (:handlers state)]
      (when (= :pending (:h/status h))
        (let [row (handler-row state hid)
              delay-ms (max 0 (- (:h/next-at h) now-ms))]
          (schedule-handler! store row delay-ms))))))

(defn- load-log!
  [state file]
  (if-not (.exists ^File file)
    state
    (with-open [r (BufferedReader. (InputStreamReader.
                                    (FileInputStream. file)
                                    "UTF-8"))]
      (loop [s state]
        (let [line (.readLine r)]
          (if (nil? line)
            s
            (let [trimmed (.trim line)]
              (if (empty? trimmed)
                (recur s)
                (let [next-state (try
                                   (let [record (edn/read-string trimmed)]
                                     (if (= :tx (:type record))
                                       (apply-tx! s record)
                                       s))
                                   (catch Throwable _
                                     s))]
                  (recur next-state))))))))))

(defn- sync-mode
  [config]
  (let [sync? (if (contains? config :sync?)
                (boolean (:sync? config))
                default-sync?)
        interval-ms (:fsync-interval-ms config)]
    (cond
      (and interval-ms (pos? (long interval-ms))) :batched
      sync? :immediate
      :else :none)))

(defn make-store
  [{:keys [filelog/config]}]
  (when-not config
    (throw (IllegalArgumentException. "Missing :filelog/config in :tx-store.")))
  (let [path (:path config)]
    (when-not path
      (throw (IllegalArgumentException. "Missing :path in :filelog/config.")))
    (let [queue-size (or (:queue-size config) default-queue-size)
          mode (sync-mode config)]
      (->FileLogStore path nil nil (ReentrantLock.) (atom (empty-state))
                      mode (:fsync-interval-ms config)
                      nil nil (atom [])
                      (LinkedBlockingQueue. (int queue-size))
                      (ScheduledThreadPoolExecutor. 1)))))

(extend-type FileLogStore
  tx-store/TxStore
  (init! [store]
    (let [file (File. ^String (:path store))
          {:keys [writer fd]} (open-writer file)
          state (load-log! (empty-state) file)
          store (assoc store :writer writer :fd fd :state (atom state))
          store (start-flusher! store)]
      (enqueue-pending-from-state! store state)
      store))

  (transact! [store tx-data]
    (let [lock ^ReentrantLock (:lock store)
          mode (:sync-mode store)
          pending (:pending-promises store)
          promise (when (= mode :batched) (promise))]
      (.lock lock)
      (try
        (write-record! store (assoc tx-data :type :tx))
        (cond
          (= mode :immediate) (sync-store! store)
          (= mode :batched) (swap! pending conj promise)
          :else nil)
        (swap! (:state store) apply-tx! tx-data)
        (finally
          (.unlock lock)))
      (when promise
        (deref promise))
      (when (= mode :immediate)
        :ok))
    (let [state @(:state store)
          now-ms (System/currentTimeMillis)]
      (doseq [h (:handlers tx-data)]
        (let [hid (:h/id h)
              row (handler-row state hid)
              delay-ms (max 0 (- (:h/next-at h) now-ms))]
          (schedule-handler! store row delay-ms)))))

  (build-tx-data [_store tx-id now events listeners]
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
                    :msg/event-type event-type
                    :msg/payload (encode-payload payload)
                    :msg/module module
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

  (query-pending-handlers [store _now]
    (let [queue ^LinkedBlockingQueue (:handler-queue store)
          batch (java.util.ArrayList.)
          max-items 1024]
      (loop [i 0]
        (if (< i max-items)
          (let [item (.poll queue)]
            (if item
              (do
                (.add batch item)
                (recur (inc i)))
              (vec batch)))
          (vec batch)))))

  (update-handler! [store update]
    (let [lock ^ReentrantLock (:lock store)]
      (.lock lock)
      (try
        (swap! (:state store) apply-handler-update! update)
        (finally
          (.unlock lock))))
    (when (= :pending (:status update))
      (let [state @(:state store)
            row (handler-row state (:handler-row-id update))
            now-ms (System/currentTimeMillis)
            delay-ms (max 0 (- (time->millis (:next-at update)) now-ms))]
        (schedule-handler! store row delay-ms))))

  (tx-status [store tx-id]
    (let [state @(:state store)
          handlers (->> (:handlers state)
                        (keep (fn [[_ h]]
                                (when-let [msg (get-in state [:msgs (:h/msg-id h)])]
                                  (when (= tx-id (:msg/tx-id msg))
                                    (:h/status h))))))]
      (cond
        (empty? handlers) :ok
        (some #{:failed :timeout} handlers) :failed
        (every? #{:ok} handlers) :ok
        :else :pending)))

  (update-tx! [store tx-id status now]
    (let [lock ^ReentrantLock (:lock store)]
      (.lock lock)
      (try
        (swap! (:state store) apply-tx-update! {:tx-id tx-id :status status :now now})
        (finally
          (.unlock lock)))))

  (cleanup! [store now retention-ok-ms]
    (let [cutoff (- (now->millis now) (long retention-ok-ms))
          state @(:state store)
          tx-ids (->> (:txs state)
                      (keep (fn [[tx-id tx]]
                              (when (and (= :ok (:tx/status tx))
                                         (< (:tx/updated-at tx) cutoff))
                                tx-id)))
                      vec)
          lock ^ReentrantLock (:lock store)]
      (if (empty? tx-ids)
        0
        (do
          (.lock lock)
          (try
            (swap! (:state store) apply-cleanup! tx-ids)
            (finally
              (.unlock lock)))
          (count tx-ids))))))
