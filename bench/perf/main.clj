(ns perf.main
  (:require [perf.cli :as cli]
            [perf.process.runner :as runner]
            [perf.scenario-registry :as registry]
            [perf.summary :as summary]
            [taoensso.timbre :as timbre]))

(defn- child-opts [opts]
  (dissoc opts :name :scenario-fn))

(defn- run-scenario-summary [{scenario-name :name scenario-fn :scenario-fn isolated-batch? :isolated-batch?} opts]
  (if (and isolated-batch? (:memory-isolated? opts))
    (runner/run-isolated-scenario-batch scenario-name (child-opts opts))
    (:summary (summary/run-scenario* (assoc opts :name scenario-name :scenario-fn scenario-fn)))))

(defn run-selected-scenarios [opts]
  (doseq [{scenario-name :name scenario-fn :scenario-fn isolated-batch? :isolated-batch?} (registry/selected-scenarios opts)]
    (summary/print-section
     (str "SCENARIO " (clojure.core/name scenario-name))
     (run-scenario-summary {:name scenario-name
                            :scenario-fn scenario-fn
                            :isolated-batch? isolated-batch?}
                           opts))))

(defn -main [& args]
  (let [opts (cli/parse-args args)]
    (when (= (:backend opts) :datahike)
      (timbre/merge-config! {:min-level :warn}))
    (summary/print-section "BENCH OPTIONS" opts)
    (let [scenarios-to-run (registry/selected-scenarios opts)]
      (when (empty? scenarios-to-run)
        (throw (IllegalArgumentException.
                (str "No scenarios selected for options: " opts))))
      (run-selected-scenarios opts))))
