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
bb bench.bb --quick --only=memory --memory-kind=peak
bb bench.bb --scenario=publish-call-latency
bb bench.bb --scenario=publish-end-to-end-latency --events=5000
bb bench.bb --timeout-min=10
bb bench.bb --only=buffered --buffer-size=512 --concurrency=8
bb bench.bb --only=memory --events=5000 --payload-bytes=2048
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
  В summary дополнительно выводятся `delivered`, `expected`, `completion-rate`.
- `publish-throughput`:
  throughput полного цикла publish -> delivery handler-у.
- `buffered-backpressure`:
  доля отказов при переполнении очереди в `:buffered`.
- `buffered-backpressure-fanout`:
  то же, но с несколькими медленными подписчиками.
  Нужен, чтобы видеть влияние fanout на buffered-поведение.
- `buffered-drain-behavior`:
  как быстро очередь опустошается после burst-нагрузки.
  В summary дополнительно выводится `processed`, а `completed?` относится к реально принятым событиям.
- `buffered-drain-fanout`:
  то же, но с несколькими подписчиками.
  Нужен для проверки, что fanout не съедает queue capacity на этапе enqueue.

### Memory

- `publish-memory-baseline`:
  грубая оценка удержания heap после серии публикаций.
  По умолчанию запускается в отдельной JVM, чтобы уменьшить влияние предыдущих сценариев.
- `publish-memory-payload`:
  то же, но с более крупным payload.
  По умолчанию запускается в отдельной JVM.
- `buffered-memory-pressure`:
  как ведет себя heap при заполнении очереди в `:buffered`.
  По умолчанию запускается в отдельной JVM.
- `buffered-memory-pressure-fanout`:
  retained memory для `:buffered` при нескольких медленных подписчиках.
  Нужен, чтобы видеть memory cost именно fanout-heavy buffered-нагрузки.
- `publish-peak-burst`:
  грубая оценка пика heap во время burst-публикаций.
  По умолчанию запускается в отдельной JVM.
- `buffered-peak-pressure`:
  грубая оценка пика heap при burst-нагрузке в `:buffered`.
  По умолчанию запускается в отдельной JVM.
- `buffered-peak-pressure-fanout`:
  peak heap для `:buffered` при нескольких медленных подписчиках.

### Transact

- `transact-throughput`
- `transact-latency`

Это пока baseline-сценарии, без глубокой детализации причин задержек.

## Параметры

- `--quick`: укороченный локальный прогон для разработки.
- `--only`: группа сценариев: `publish`, `buffered`, `memory`, `transact`, `all`.
- `--scenario`: запустить один конкретный сценарий.
- `--mode`: `unlimited` или `buffered` (по умолчанию `unlimited`).
  Для `--only=buffered` отдельный `--mode=buffered` не требуется: эти сценарии сами создают buffered bus.
- `--buffer-size`: размер очереди в buffered режиме (по умолчанию `1024`).
- `--concurrency`: количество worker‑ов в buffered режиме (по умолчанию `4`).
- `--subscribers`: число подписчиков для publish тестов (по умолчанию `1`).
- Для fanout-сценариев `buffered-*fanout` значение `--subscribers` поднимается как минимум до `8`,
  чтобы сценарий действительно был чувствителен к fanout.
- `--events`: количество событий для throughput‑тестов publish (по умолчанию `10000`).
- `--latency-samples`: число измерений для latency (по умолчанию `1000`).
- `--payload-bytes`: размер служебного payload для publish/memory тестов.
- `--warmup-iterations`: число прогревающих прогонов перед измерением.
- `--measure-iterations`: число измеряемых прогонов.
- `--drain-timeout-ms`: сколько ждать завершения обработки после публикации.
- `--memory-isolated`: `true` или `false`; по умолчанию `true`.
  Если `true`, memory-сценарии запускаются в отдельном процессе JVM.
- `--memory-kind`: `retained`, `peak` или `all`; по умолчанию `retained`.
  Используется для `--only=memory`, чтобы выбрать тип memory-метрик.
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
- По умолчанию memory-сценарии изолируются в отдельной JVM, чтобы уменьшить шум от предыдущих benchmark-ов в том же процессе.
- `retained` и `peak` отвечают на разные вопросы и не должны смешиваться при интерпретации.
- Memory-результаты полезны для сравнения режимов и поиска явного удержания памяти, но это не allocation profiler.
- При `--only=memory` buffered memory-сценарии тоже запускаются; отдельный `--mode=buffered` для этого не требуется.
