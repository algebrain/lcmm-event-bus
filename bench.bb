#!/usr/bin/env bb
(ns bench
  (:require [babashka.process :refer [process]]
            [clojure.string :as str])
  (:import [java.util.concurrent TimeUnit]))

(defn- parse-long [s]
  (try
    (Long/parseLong s)
    (catch Throwable _ nil)))

(defn- parse-timeout-ms [args]
  (let [timeout-ms-arg (some (fn [arg]
                               (when (str/starts-with? arg "--timeout-ms=")
                                 (subs arg (count "--timeout-ms="))))
                             args)
        timeout-min-arg (some (fn [arg]
                                (when (str/starts-with? arg "--timeout-min=")
                                  (subs arg (count "--timeout-min="))))
                              args)]
    (cond
      timeout-ms-arg (parse-long timeout-ms-arg)
      timeout-min-arg (some-> (parse-long timeout-min-arg) (* 60 1000))
      :else (* 5 60 1000))))

(def timeout-ms
  (or (parse-timeout-ms *command-line-args*)
      (* 5 60 1000)))

(defn- bench-args [args]
  (remove #(or (str/starts-with? % "--timeout-ms=")
               (str/starts-with? % "--timeout-min="))
          args))

(defn run! [cmd]
  (let [proc (process {:inherit true} cmd)
        ^Process p (:proc proc)
        finished? (.waitFor p timeout-ms TimeUnit/MILLISECONDS)]
    (if finished?
      (let [exit (.exitValue p)]
        (when (not= 0 exit)
          (System/exit exit)))
      (do
        (.destroyForcibly p)
        (.waitFor p 5000 TimeUnit/MILLISECONDS)
        (System/exit 1)))))

(defn -main [& _]
  (let [args (bench-args *command-line-args*)
        suffix (if (seq args) (str " -- " (str/join " " args)) "")]
    (run! (str "clj -J--enable-native-access=ALL-UNNAMED -M:bench-perf -m perf" suffix))))

(apply -main *command-line-args*)
