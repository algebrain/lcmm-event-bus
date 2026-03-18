(ns perf.util
  (:import [java.lang.management ManagementFactory]
           [java.util.concurrent CountDownLatch TimeUnit]))

(defn now-ns []
  (System/nanoTime))

(defn nanos->ms [n]
  (/ (double n) 1000000.0))

(defn build-payload [payload-bytes seed]
  (let [base (str "payload-" seed "-")
        needed (max 1 (long payload-bytes))
        repeated (apply str (take (inc (quot needed (count base))) (repeat base)))]
    {:n seed
     :blob (subs repeated 0 needed)}))

(defn await-latch [^CountDownLatch latch timeout-ms]
  (.await latch timeout-ms TimeUnit/MILLISECONDS))

(defn await-condition [timeout-ms pred]
  (let [deadline (+ (System/currentTimeMillis) timeout-ms)]
    (loop []
      (cond
        (pred) true
        (< (System/currentTimeMillis) deadline) (do (Thread/sleep 1) (recur))
        :else false))))

(defn gc-stats []
  (reduce
   (fn [acc bean]
     (-> acc
         (update :gc-count + (max 0 (.getCollectionCount bean)))
         (update :gc-time-ms + (max 0 (.getCollectionTime bean)))))
   {:gc-count 0 :gc-time-ms 0}
   (ManagementFactory/getGarbageCollectorMXBeans)))

(defn used-heap-bytes []
  (let [rt (Runtime/getRuntime)]
    (- (.totalMemory rt) (.freeMemory rt))))

(defn settle-memory! []
  (dotimes [_ 3]
    (System/gc)
    (Thread/sleep 50)))

(defn capture-memory []
  (merge {:used-heap-bytes (used-heap-bytes)}
         (gc-stats)))

(defn bytes->kb [n]
  (/ (double n) 1024.0))

(defn memory-summary [before after]
  (let [delta-bytes (- (:used-heap-bytes after) (:used-heap-bytes before))]
    {:used-heap-before-kb (bytes->kb (:used-heap-bytes before))
     :used-heap-after-kb (bytes->kb (:used-heap-bytes after))
     :heap-delta-kb (bytes->kb delta-bytes)
     :gc-count-delta (- (:gc-count after) (:gc-count before))
     :gc-time-delta-ms (- (:gc-time-ms after) (:gc-time-ms before))}))
