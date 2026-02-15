@echo off
clj -M:test --reporter kaocha.report/documentation
