#!/usr/bin/env bb
(ns test
  (:require [babashka.process :refer [shell]]))

(def green "\u001b[1;32m")
(def reset "\u001b[0m")

(defn banner [text]
  (println (str green text reset)))

(defn run! [cmd]
  (let [{:keys [exit]} (shell {:inherit true} cmd)]
    (when (not= 0 exit)
      (System/exit exit))))

(banner "LINT")
(run! "clj -J--enable-native-access=ALL-UNNAMED -M:lint")

(banner "TESTS")
(run! "clj -J--enable-native-access=ALL-UNNAMED -M:test --reporter kaocha.report/documentation")

(banner "FORMAT")
(run! "clj -J--enable-native-access=ALL-UNNAMED -M:format")
