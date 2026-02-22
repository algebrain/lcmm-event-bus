# BENCH

Набор минимальных бенчмарков для оценки латентности и пропускной способности `event-bus`.

## Запуск

```powershell
bb bench.bb
```

Примеры:

```powershell
bb bench.bb --timeout-min=10
bb bench.bb --mode=buffered --buffer-size=512 --concurrency=8
bb bench.bb --events=20000 --latency-samples=2000
bb bench.bb --backend=sqlite --tx-count=200 --tx-batch=10
```

## Параметры

- `--mode`: `unlimited` или `buffered` (по умолчанию `unlimited`).
- `--buffer-size`: размер очереди в buffered режиме (по умолчанию `1024`).
- `--concurrency`: количество worker‑ов в buffered режиме (по умолчанию `4`).
- `--subscribers`: число подписчиков для publish тестов (по умолчанию `1`).
- `--events`: количество событий для throughput‑тестов publish (по умолчанию `10000`).
- `--latency-samples`: число измерений для latency (по умолчанию `1000`).
- `--tx-count`: число транзакций в throughput‑тесте transact (по умолчанию `200`).
- `--tx-batch`: число событий в одной transact‑транзакции (по умолчанию `1`).
- `--backend`: `datahike` или `sqlite` (по умолчанию `sqlite`).
- `--handler-backoff-ms`: задержка между ретраями transact (по умолчанию `10`).
- `--handler-max-retries`: максимум ретраев transact (по умолчанию `2`).
- `--tx-timeout-ms`: таймаут ожидания результата transact в бенчмарках (по умолчанию `2000`).
- `--timeout-min` / `--timeout-ms`: общий таймаут выполнения бенчмарка (по умолчанию `5 минут`).

## Замечания

- Результаты зависят от железа и JVM, используйте как ориентир.
- Бенчмарки не входят в `bb test.bb`.
- При `--backend=datahike` уровень логов для бенчмарков снижается до `WARN`, чтобы не засорять вывод.
