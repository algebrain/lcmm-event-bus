@echo off
clj -J--enable-native-access=ALL-UNNAMED -M:lint
clj -J--enable-native-access=ALL-UNNAMED -M:test --reporter kaocha.report/documentation
clj -J--enable-native-access=ALL-UNNAMED -M:format
