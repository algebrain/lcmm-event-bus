# Документация по API: event-bus

Репозиторий: `github.com/algebrain/lcmm-event-bus`

Этот документ описывает публичный API для `event-bus` и его обычное место в LCMM-приложении.


## 1. Где шина живет в обычном приложении

В реальном LCMM-приложении шина обычно создается не внутри модуля, а на уровне приложения.

Обычно картина такая:

1. приложение собирает общий реестр схем событий;
2. приложение создает `event-bus`;
3. приложение передает шину в модули через зависимости;
4. HTTP-обработчик может опубликовать первое событие в цепочке;
5. следующие обработчики публикуют новые события уже с `:parent-envelope`.

Небольшой пример:

```clojure
(ns my.app.system
  (:require [event-bus :as bus]))

(defn make-schema-registry []
  {:booking/create-requested {"1.0" [:map [:slot-id :string] [:user-id :string]]}
   :booking/created {"1.0" [:map [:booking-id :string] [:slot-id :string] [:user-id :string]]}
   :notify/booking-created {"1.0" [:map [:booking-id :string] [:message :string]]}})

(defn make-system [logger]
  (let [event-bus (bus/make-bus
                   :schema-registry (make-schema-registry)
                   :logger logger
                   :log-payload :none)]
    {:bus event-bus}))
```

Смысл этого примера простой:

1. схемы событий собираются в одном месте;
2. шина создается в корне приложения;
3. модули получают уже готовую шину и не поднимают свою отдельную.

## 2. Быстрый старт

Минимальный рабочий пример:

```clojure
(require '[event-bus :as bus])
(require '[malli.core :as m])

(def registry
  {:demo/ping {"1.0" (m/schema [:map [:msg :string]])}})

(def b (bus/make-bus :schema-registry registry))

(bus/subscribe b :demo/ping
               (fn [_ envelope]
                 (println "Got:" (-> envelope :payload :msg))))

(bus/publish b :demo/ping {:msg "hello"} {:module :demo})
```

## 3. Как начинается и продолжается цепочка событий

Для обычного приложения важно понимать не только отдельные вызовы API, но и общий ход событий.

### 3.1 Первое событие из HTTP-обработчика

HTTP-обработчик вполне может быть началом событийной цепочки.

```clojure
(ns my.app.booking-http
  (:require [event-bus :as bus]
            [lcmm.http.core :as http]))

(defn create-booking-handler [bus request]
  (bus/publish bus
               :booking/create-requested
               {:slot-id (get-in request [:query-params "slot-id"])
                :user-id (get-in request [:query-params "user-id"])}
               (http/->bus-publish-opts request {:module :booking}))
  {:status 200
   :body "ok"})
```

Что здесь важно:

1. HTTP-запрос может породить первое событие без дополнительной сложной обвязки;
2. это событие уже получает свой `correlation-id`;
3. дальше по этому идентификатору можно связать всю цепочку.

### 3.2 Следующее событие из обработчика модуля

Если модуль реагирует на событие и публикует следующее, он не должен начинать новую цепочку заново.

```clojure
(ns my.app.notify
  (:require [event-bus :as bus]))

(defn on-booking-created [bus envelope]
  (bus/publish bus
               :notify/booking-created
               {:booking-id (-> envelope :payload :booking-id)
                :message "booking-created"}
               {:parent-envelope envelope
                :module :notify}))
```

Смысл `:parent-envelope` такой:

1. сохраняется тот же `correlation-id`;
2. причинно-следственная цепочка продолжает расти;
3. позже можно понять, какое событие породило следующее.

Короткий пример того, как выглядит цепочка:

```clojure
{:event-type :booking/create-requested
 :module :booking
 :correlation-id #uuid "..."
 :causation-path []}

{:event-type :notify/booking-created
 :module :notify
 :correlation-id #uuid "..."
 :causation-path [[:booking :booking/created]]}
```

## 4. Конструктор

### `make-bus`

Создает новый экземпляр шины.

```clojure
(require '[event-bus :as bus])

(def a-bus
  (bus/make-bus
    :schema-registry {:demo/ping {"1.0" [:map [:msg :string]]}}))
```

#### Опции

Можно передать опции как именованные аргументы.
`make-bus` бросает исключение, если не указан `:schema-registry`.

- `:mode`: `:unlimited` (по умолчанию) или `:buffered`.
- `:max-depth`: максимальная глубина цепочки событий (по умолчанию `20`).
- `:schema-registry`: обязательный реестр схем событий.
- `:logger`: функция для логирования с видом `(fn [level data])`.
- `:buffer-size`: для режима `:buffered`, размер очереди (по умолчанию `1024`).
- `:concurrency`: для режима `:buffered`, количество потоков-обработчиков (по умолчанию `4`).
- `:tx-store`: конфигурация внутренней БД для `transact` (по умолчанию используется SQLite; также доступны `:sqlite`, `:datahike`, `:filelog`, см. `TRANSACT.md`).
- `:tx-handler-timeout`: таймаут обработчика в `transact` в миллисекундах (по умолчанию `10000`).
- `:handler-max-retries`: количество повторов обработчика в `transact` (по умолчанию `3`).
- `:handler-backoff-ms`: задержка между повторами в `transact` в миллисекундах (по умолчанию `1000`).
- `:log-payload`: режим логирования payload (`:none`, `:keys`, `:truncated`; по умолчанию `:none`).
- `:log-payload-max-chars`: максимальная длина payload в логах (по умолчанию `1024`).
- `:payload-dump`: запись payload в файл при ошибках обработчика (map с ключами `:on-events`, `:dir`, `:max-bytes`, `:redact`).
- `:tx-retention-ms`: срок хранения успешных транзакций `transact` в миллисекундах (по умолчанию `7 дней`).
- `:tx-cleanup-interval-ms`: период фоновой очистки в миллисекундах (по умолчанию `1 час`).

Параметры `:tx-store`, `:tx-handler-timeout`, `:handler-max-retries`, `:handler-backoff-ms` используются только если включен `transact`.

Пример с опциями:

```clojure
(def buffered-bus
  (bus/make-bus
    :mode :buffered
    :buffer-size 500
    :concurrency 8
    :schema-registry {:user/created {"1.0" [:map [:id :int] [:email :string]]}}
    :logger (fn [level data]
              (println "LOG:" level data))))
```

## 5. Основные функции

### `subscribe`

Подписывает обработчик на определенный тип события.

- Сигнатура: `(subscribe bus event-type handler & {:keys [schema meta]})`
- `event-type`: ключевое слово, которое обозначает событие, например `:order/created`.
- `handler`: функция вида `(fn [bus envelope])`. Такой вид важен, если обработчик будет публиковать следующие события.
- `:schema`: необязательная схема `malli` для проверки payload. Если проверка не проходит, обработчик вызван не будет.

Пример:

```clojure
(bus/subscribe a-bus
               :user/registered
               (fn [bus envelope]
                 (println "New user:" (:payload envelope))
                 (bus/publish bus
                              :email/send-welcome
                              (:payload envelope)
                              {:parent-envelope envelope
                               :module :mailer})))
```

### `publish`

Публикует событие в шине.

- Сигнатура: `(publish bus event-type payload & [opts])`
- `payload`: данные события, обычно карта.
- `opts`: карта опций. Внутри обязателен `:module`, а `:schema-version` указывается при необходимости.
- Возвращаемое значение: функция возвращает созданный конверт события `envelope`. В нем лежат и данные, и служебные поля вроде `message-id`, `correlation-id` и `causation-path`.

Проверка в `publish` выполняется строго по реестру схем, переданному в `make-bus`.
Если схемы нет или payload не проходит проверку, `publish` бросает исключение.

#### Опции для `publish`

- `:parent-envelope`: используйте, если публикуете событие в ответ на другое. Шина возьмет оттуда `correlation-id` и продолжит `causation-path`.
- `:module`: обязательная опция. Это идентификатор модуля-инициатора. Он используется и для отслеживания цепочки, и для защиты от циклов по паре `(module, event-type)`.
- `:schema-version`: необязательная версия схемы. По умолчанию используется `"1.0"`.

Пример корневого события:

```clojure
(bus/publish a-bus :user/logged-in {:user-id 123} {:module :auth})
```

Пример следующего события:

```clojure
(fn [bus envelope]
  (let [user-id (-> envelope :payload :user-id)]
    (bus/publish bus
                 :audit/user-activity
                 {:activity "login" :user user-id}
                 {:parent-envelope envelope
                  :module :audit})))
```

### `transact`

Атомарно записывает список событий во внутреннее хранилище шины и запускает их обработку.

Простой пример транзакционной публикации:

```clojure
(bus/transact a-bus
  [{:event-type :user/created
    :payload {:user-id 42 :email "a@b.com"}
    :module :user}])
```

Возвращает карту как минимум с ключами:

- `:op-id`
- `:result-promise`
- `:result-chan`
- `:result-mult`

Подробное описание `transact`, конфигурации `:tx-store` и контракта обработчиков см. в [`TRANSACT.md`](./TRANSACT.md).

### `unsubscribe`

Отписывает обработчик от события.

- Сигнатура: `(unsubscribe bus event-type matcher)`
- `matcher`: либо сама функция-обработчик, либо метаданные `:meta`, которые были переданы при подписке.

Пример:

```clojure
(defn my-handler [bus envelope]
  (println "Called!"))

(bus/subscribe a-bus :my/event my-handler)

(bus/unsubscribe a-bus :my/event my-handler)
```

### `clear-listeners`

Очищает подписки.

Есть два варианта вызова:

- `(clear-listeners bus)` — удалить все подписки у всей шины;
- `(clear-listeners bus event-type)` — удалить подписки только у одного типа события.

Пример:

```clojure
(bus/clear-listeners a-bus :my/event)
(bus/clear-listeners a-bus)
```

### `close`

Закрывает шину и останавливает ее внутренние рабочие потоки.

Есть два варианта вызова:

- `(close bus)`
- `(close bus {:timeout 10000})`

`timeout` задается в миллисекундах и означает, сколько ждать аккуратного завершения работы.

Пример:

```clojure
(bus/close a-bus)
```

### `listener-count`

Возвращает число подписок.

Есть два варианта вызова:

- `(listener-count bus)` — число всех подписок в шине;
- `(listener-count bus event-type)` — число подписок на конкретное событие.

Пример:

```clojure
(bus/listener-count a-bus)
(bus/listener-count a-bus :my/event)
```

## 7. Конверт сообщения

`envelope` — это карта с метаданными и полезной нагрузкой.

- `:message-id` — UUID сообщения.
- `:correlation-id` — UUID цепочки событий.
- `:causation-path` — вектор пар `[module event-type]`.
- `:event-type` — тип события.
- `:module` — модуль-инициатор.
- `:schema-version` — версия схемы, по умолчанию `"1.0"`.
- `:payload` — полезная нагрузка события.

Если событие публикуется с `:parent-envelope`, `correlation-id` сохраняется, а `causation-path` расширяется.

## 8. Ошибки и проверка данных

- `publish` проверяет `payload` строго по `:schema-registry`. Если схемы нет или данные невалидны, будет выброшено исключение.
- `:schema` в `subscribe` влияет только на вызов обработчика. Проверка в `publish` от subscriber-схем не зависит.
- Возвращаемое значение обработчика для обычного `publish` игнорируется. Для `transact` обработчик должен вернуть `true`.

