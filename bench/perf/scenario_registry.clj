(ns perf.scenario-registry
  (:require [perf.process.runner :as runner]
            [perf.scenarios.buffered :as buffered]
            [perf.scenarios.memory-peak :as memory-peak]
            [perf.scenarios.memory-retained :as memory-retained]
            [perf.scenarios.publish :as publish]
            [perf.scenarios.transact :as transact]))

(defn- child-opts [opts]
  (dissoc opts :name :scenario-fn))

(defn memory-scenario [name kind scenario-fn]
  {:name name
   :group :memory
   :memory-kind kind
   :isolated-batch? true
   :local-scenario-fn scenario-fn
   :scenario-fn (fn [opts]
                  (if (:memory-isolated? opts)
                    (runner/run-isolated-scenario name (child-opts opts))
                    (scenario-fn opts)))})

(def scenarios
  [{:name :publish-call-latency
    :group :publish
    :scenario-fn publish/publish-call-latency-run}
   {:name :publish-end-to-end-latency
    :group :publish
    :scenario-fn publish/publish-end-to-end-latency-run}
   {:name :publish-throughput
    :group :publish
    :scenario-fn publish/publish-throughput-run}
   {:name :buffered-backpressure
    :group :buffered
    :scenario-fn buffered/buffered-backpressure-run
    :modes #{:buffered}}
   {:name :buffered-backpressure-fanout
    :group :buffered
    :scenario-fn buffered/buffered-backpressure-fanout-run
    :modes #{:buffered}}
   {:name :buffered-drain-behavior
    :group :buffered
    :scenario-fn buffered/buffered-drain-run
    :modes #{:buffered}}
   {:name :buffered-drain-fanout
    :group :buffered
    :scenario-fn buffered/buffered-drain-fanout-run
    :modes #{:buffered}}
   (memory-scenario :publish-memory-baseline :retained memory-retained/publish-memory-baseline-run)
   (memory-scenario :publish-memory-payload :retained memory-retained/publish-memory-payload-run)
   (assoc (memory-scenario :buffered-memory-pressure :retained memory-retained/buffered-memory-pressure-run)
          :modes #{:buffered})
   (assoc (memory-scenario :buffered-memory-pressure-fanout :retained memory-retained/buffered-memory-pressure-fanout-run)
          :modes #{:buffered})
   (memory-scenario :publish-peak-burst :peak memory-peak/publish-peak-burst-run)
   (assoc (memory-scenario :buffered-peak-pressure :peak memory-peak/buffered-peak-pressure-run)
          :modes #{:buffered})
   (assoc (memory-scenario :buffered-peak-pressure-fanout :peak memory-peak/buffered-peak-pressure-fanout-run)
          :modes #{:buffered})
   {:name :transact-throughput
    :group :transact
    :scenario-fn transact/transact-throughput-run}
   {:name :transact-latency
    :group :transact
    :scenario-fn transact/transact-latency-run}])

(defn mode-selected? [{:keys [group name modes]} {:keys [only mode scenario]}]
  (or (nil? modes)
      (contains? modes mode)
      (= only group)
      (= scenario name)))

(defn memory-kind-selected? [{:keys [group memory-kind]} {:keys [only scenario desired-kind]}]
  (or (not= group :memory)
      (some? scenario)
      (not= only :memory)
      (= desired-kind :all)
      (= memory-kind desired-kind)))

(defn scenario-enabled? [{:keys [group name] :as scenario-def} {:keys [only scenario memory-kind] :as opts}]
  (and
   (or (nil? scenario) (= scenario name))
   (or (= only :all) (= only group))
   (mode-selected? scenario-def opts)
   (memory-kind-selected? scenario-def (assoc opts :desired-kind memory-kind))))

(defn selected-scenarios [opts]
  (vec (filter #(scenario-enabled? % opts) scenarios)))
