# BENCH

Набор локальных бенчмарков для оценки времени и памяти в `event-bus`.

Сейчас основной фокус у бенчмарков на `publish` и `buffered`.
`transact` остается в наборе, но глубоко пока не прорабатывается.

## Запуск

```powershell
bb bench.bb
```

Примеры:

```powershell
bb bench.bb --quick
bb bench.bb --only=memory
bb bench.bb --quick --only=memory
bb bench.bb --scenario=publish-call-latency
bb bench.bb --scenario=publish-end-to-end-latency --events=5000
bb bench.bb --timeout-min=10
bb bench.bb --mode=buffered --buffer-size=512 --concurrency=8
bb bench.bb --mode=buffered --only=memory --events=5000 --payload-bytes=2048
bb bench.bb --events=20000 --latency-samples=2000 --subscribers=8
bb bench.bb --backend=sqlite --tx-count=200 --tx-batch=10
bb bench.bb --backend=filelog --fsync-interval-ms=2
```

## Что именно есть сейчас

### Publish / buffered

- `publish-call-latency`:
  стоимость самого вызова `publish`, без ожидания входа в handler.
- `publish-end-to-end-latency`:
  время от вызова `publish` до входа в handler.
- `publish-throughput`:
  throughput полного цикла publish -> delivery handler-у.
- `buffered-backpressure`:
  доля отказов при переполнении очереди в `:buffered`.
- `buffered-drain-behavior`:
  как быстро очередь опустошается после burst-нагрузки.

### Memory

- `publish-memory-baseline`:
  грубая оценка удержания heap после серии публикаций.
- `publish-memory-payload`:
  то же, но с более крупным payload.
- `buffered-memory-pressure`:
  как ведет себя heap при заполнении очереди в `:buffered`.

### Transact

- `transact-throughput`
- `transact-latency`

Это пока baseline-сценарии, без глубокой детализации причин задержек.

## Параметры

- `--quick`: укороченный локальный прогон для разработки.
- `--only`: группа сценариев: `publish`, `buffered`, `memory`, `transact`, `all`.
- `--scenario`: запустить один конкретный сценарий.
- `--mode`: `unlimited` или `buffered` (по умолчанию `unlimited`).
- `--buffer-size`: размер очереди в buffered режиме (по умолчанию `1024`).
- `--concurrency`: количество worker‑ов в buffered режиме (по умолчанию `4`).
- `--subscribers`: число подписчиков для publish тестов (по умолчанию `1`).
- `--events`: количество событий для throughput‑тестов publish (по умолчанию `10000`).
- `--latency-samples`: число измерений для latency (по умолчанию `1000`).
- `--payload-bytes`: размер служебного payload для publish/memory тестов.
- `--warmup-iterations`: число прогревающих прогонов перед измерением.
- `--measure-iterations`: число измеряемых прогонов.
- `--drain-timeout-ms`: сколько ждать завершения обработки после публикации.
- `--tx-count`: число транзакций в throughput‑тесте transact (по умолчанию `200`).
- `--tx-batch`: число событий в одной transact‑транзакции (по умолчанию `1`).
- `--backend`: `datahike`, `sqlite` или `filelog` (по умолчанию `sqlite`).
- `--fsync-interval-ms`: батч‑fsync для `filelog` (мс).
- `--handler-backoff-ms`: задержка между ретраями transact (по умолчанию `10`).
- `--handler-max-retries`: максимум ретраев transact (по умолчанию `2`).
- `--tx-timeout-ms`: таймаут ожидания результата transact в бенчмарках (по умолчанию `2000`).
- `--timeout-min` / `--timeout-ms`: общий таймаут выполнения бенчмарка (по умолчанию `5 минут`).

## Замечания

- Результаты зависят от железа и JVM, используйте как ориентир.
- Бенчмарки не входят в `bb test.bb`.
- При `--backend=datahike` уровень логов для бенчмарков снижается до `WARN`, чтобы не засорять вывод.
- Memory-сценарии используют грубую локальную оценку heap через JVM runtime и GC MXBeans.
- Memory-результаты полезны для сравнения режимов и поиска явного удержания памяти, но это не allocation profiler.
- `--only=memory` в режиме `--mode=unlimited` не запускает buffered-only memory-сценарии.
