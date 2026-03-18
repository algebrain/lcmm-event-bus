(ns perf.process.child-main
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [perf.scenarios.memory-peak :as memory-peak]
            [perf.scenarios.memory-retained :as memory-retained]
            [perf.summary :as summary]))

(def child-scenarios
  (merge memory-retained/child-scenarios
         {:publish-peak-burst memory-peak/publish-peak-burst-run
          :buffered-peak-pressure memory-peak/buffered-peak-pressure-run}))

(defn- parse-child-args [args]
  (loop [[arg & more] args
         acc {}]
    (if (nil? arg)
      acc
      (case arg
        "--batch" (recur more (assoc acc :batch? true))
        "--scenario" (let [[value & rest] more]
                       (recur rest (assoc acc :scenario (keyword value))))
        "--opts-file" (let [[value & rest] more]
                        (recur rest (assoc acc :opts (edn/read-string (slurp (io/file value))))))
        (throw (IllegalArgumentException. (str "Unsupported child arg: " arg)))))))

(defn -main [& args]
  (let [{:keys [scenario opts batch?]} (parse-child-args args)
        scenario-fn (get child-scenarios scenario)]
    (when-not scenario-fn
      (throw (IllegalArgumentException.
              (str "Unsupported child scenario: " scenario))))
    (println
     (pr-str
      (if batch?
        (:summary (summary/run-scenario* (assoc opts :name scenario :scenario-fn scenario-fn)))
        (scenario-fn opts))))))
