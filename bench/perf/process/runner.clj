(ns perf.process.runner
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as str]))

(defn- read-stream [stream]
  (slurp stream))

(defn- child-command [scenario-name opts-file]
  ["clj"
   "-J--enable-native-access=ALL-UNNAMED"
   "-M:bench-perf"
   "-m"
   "perf.process.child-main"
   "--scenario"
   (name scenario-name)
   "--opts-file"
   opts-file])

(defn- run-child! [scenario-name opts extra-args]
  (let [tmp-file (java.io.File/createTempFile "event-bus-bench-opts-" ".edn")
        _ (spit tmp-file (pr-str opts))
        cmd (into (child-command scenario-name (.getAbsolutePath tmp-file)) extra-args)
        builder (ProcessBuilder. ^java.util.List cmd)
        _ (.directory builder (java.io.File. "."))
        _ (.redirectErrorStream builder false)]
    (try
      (let [process (.start builder)
            stdout (future (read-stream (.getInputStream process)))
            stderr (future (read-stream (.getErrorStream process)))
            exit-code (.waitFor process)
            out @stdout
            err @stderr]
        (when-not (zero? exit-code)
          (throw (ex-info (str "Isolated scenario failed: " (name scenario-name))
                          {:scenario scenario-name
                           :exit-code exit-code
                           :stdout out
                           :stderr err})))
        (let [line (or (last (remove str/blank? (str/split-lines out)))
                       (throw (ex-info (str "Empty output from isolated scenario " (name scenario-name))
                                       {:scenario scenario-name
                                        :stdout out
                                        :stderr err})))]
          (edn/read-string line)))
      (finally
        (io/delete-file tmp-file true)))))

(defn run-isolated-scenario [scenario-name opts]
  (run-child! scenario-name opts []))

(defn run-isolated-scenario-batch [scenario-name opts]
  (run-child! scenario-name opts ["--batch"]))
