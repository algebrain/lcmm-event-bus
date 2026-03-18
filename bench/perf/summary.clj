(ns perf.summary)

(defn percentile [sorted-values p]
  (let [n (count sorted-values)]
    (if (zero? n)
      0.0
      (let [idx (long (Math/floor (* (dec n) p)))]
        (double (nth sorted-values idx))))))

(defn stats [values]
  (let [sorted (vec (sort values))]
    {:count (count sorted)
     :min (if (seq sorted) (double (first sorted)) 0.0)
     :p50 (percentile sorted 0.50)
     :p95 (percentile sorted 0.95)
     :p99 (percentile sorted 0.99)
     :max (if (seq sorted) (double (peek sorted)) 0.0)
     :avg (if (seq sorted)
            (/ (reduce + 0.0 sorted) (double (count sorted)))
            0.0)}))

(defn average [values]
  (if (seq values)
    (/ (reduce + 0.0 values) (double (count values)))
    0.0))

(defn aggregate-runs [runs]
  (reduce
   (fn [acc run]
     (reduce-kv
      (fn [m k v]
        (if (number? v)
          (update m k (fnil conj []) (double v))
          (assoc m k v)))
      acc
      run))
   {}
   runs))

(defn summarize-runs [runs]
  (let [aggregated (aggregate-runs runs)]
    (into {:runs (count runs)}
          (map (fn [[k v]]
                 (if (vector? v)
                   [k (average v)]
                   [k v])))
          aggregated)))

(defn run-scenario* [{:keys [name warmup-iterations measure-iterations scenario-fn] :as opts}]
  (dotimes [_ warmup-iterations]
    (scenario-fn opts))
  (let [runs (vec (repeatedly measure-iterations #(scenario-fn opts)))]
    {:scenario name
     :summary (summarize-runs runs)
     :runs runs}))

(defn print-section [title data]
  (println)
  (println title)
  (println (pr-str data)))
