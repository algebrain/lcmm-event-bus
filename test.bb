#!/usr/bin/env bb
(ns test
  (:require [babashka.process :refer [process]]
            [clojure.string :as str])
  (:import [java.util.concurrent TimeUnit]))

(def green "[1;32m")
(def reset "[0m")

(defn started-at []
  (let [t (java.time.LocalTime/now)
        s (.format t (java.time.format.DateTimeFormatter/ofPattern "HH:mm"))]
    (println (str green "STARTED AT " s reset))))

(defn banner [text]
  (println (str green text reset)))

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

(started-at)
(banner "LINT")
(run! "clj -J--enable-native-access=ALL-UNNAMED -M:lint")

(banner "TESTS")
(run! "clj -J--enable-native-access=ALL-UNNAMED -M:test --reporter kaocha.report/documentation")

(banner "FORMAT")
(run! "clj -J--enable-native-access=ALL-UNNAMED -M:format")
