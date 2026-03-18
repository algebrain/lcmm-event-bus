#!/usr/bin/env bb
(ns bench
  (:require [babashka.process :refer [process]]
            [clojure.string :as str])
  (:import [java.util.concurrent TimeUnit]))

(defn- parse-long [s]
  (try
    (Long/parseLong s)
    (catch Throwable _ nil)))

(def green "[1;32m")
(def reset "[0m")

(defn started-at []
  (let [t (java.time.LocalTime/now)
        s (.format t (java.time.format.DateTimeFormatter/ofPattern "HH:mm"))]
    (println (str green "STARTED AT " s reset))))

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

(defn- has-flag? [args flag]
  (boolean (some #(= % flag) args)))

(defn- has-prefix? [args prefix]
  (boolean (some #(str/starts-with? % prefix) args)))

(defn- maybe-add-defaults [args]
  (cond-> (vec args)
    (has-flag? args "--quick")
    (#(cond-> %
        (not (has-prefix? % "--warmup-iterations=")) (conj "--warmup-iterations=1")
        (not (has-prefix? % "--measure-iterations=")) (conj "--measure-iterations=1")))))

(defn- destroy-tree! [^Process p]
  (let [^java.lang.ProcessHandle ph (.toHandle p)
        consumer (reify java.util.function.Consumer
                   (accept [_ ^java.lang.ProcessHandle h]
                     (.destroyForcibly h)))]
    (.forEach (.descendants ph) consumer)
    (.destroyForcibly p)))

(defn run! [cmd]
  (let [proc (process {:inherit true} cmd)
        ^Process p (:proc proc)
        finished? (.waitFor p timeout-ms TimeUnit/MILLISECONDS)]
    (if finished?
      (let [exit (.exitValue p)]
        (when (not= 0 exit)
          (System/exit exit)))
      (do
        (println (str green "TIMEOUT after " timeout-ms " ms: " cmd reset))
        (destroy-tree! p)
        (.waitFor p 5000 TimeUnit/MILLISECONDS)
        (when (.isAlive p)
          (println (str green "Process still alive after forced destroy" reset)))
        (System/exit 1)))))

(defn -main [& _]
  (started-at)
  (let [args (-> *command-line-args* bench-args maybe-add-defaults)
        suffix (if (seq args) (str " -- " (str/join " " args)) "")]
    (run! (str "clj -J--enable-native-access=ALL-UNNAMED -M:bench-perf -m perf.main" suffix))))

(apply -main *command-line-args*)
