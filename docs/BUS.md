# Документация по API: event-bus

Этот документ описывает публичный API для `event-bus`.

## Быстрый старт

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

## Конструктор

### `make-bus`

Создает новый экземпляр шины.

```clojure
(require '[event-bus :as bus])

(def a-bus (bus/make-bus))
```

#### Опции

Вы можете передать опции как именованные аргументы.  
`make-bus` бросает исключение, если не указан `:schema-registry`.

- `:mode`: `:unlimited` (по умолчанию) или `:buffered`.
- `:max-depth`: Максимальная глубина цепочки событий (по умолчанию: `20`).
- `:schema-registry`: **Обязательный** реестр схем событий (map of versions).
- `:logger`: Функция для логирования, принимающая `(fn [level data])`.
- `:buffer-size`: (для режима `:buffered`) Размер очереди (по умолчанию: `1024`).
- `:concurrency`: (для режима `:buffered`) Количество потоков-обработчиков (по умолчанию: `4`).
- `:tx-store`: Конфигурация внутренней БД для `transact` (см. `TRANSACT.md`).
- `:tx-handler-timeout`: Таймаут обработчика в `transact` (мс, по умолчанию: `10000`).
- `:handler-max-retries`: Количество ретраев обработчика в `transact` (по умолчанию: `3`).
- `:handler-backoff-ms`: Задержка между ретраями (мс, по умолчанию: `1000`).
- `:tx-retention-ms`: Срок хранения успешных транзакций `transact` (мс, по умолчанию: `7 дней`).
- `:tx-cleanup-interval-ms`: Периодичность фоновой очистки (мс, по умолчанию: `1 час`).

Параметры `:tx-store`, `:tx-handler-timeout`, `:handler-max-retries`, `:handler-backoff-ms` используются только если включен `transact`.

**Пример с опциями:**
```clojure
(def buffered-bus
  (bus/make-bus
    :mode :buffered
    :buffer-size 500
    :concurrency 8
    :schema-registry {:user/created {"1.0" [:map [:id :int] [:email :string]]}}
    :logger (fn [lvl d] (println "LOG:" lvl d))))
```

## Основные функции

### `subscribe`

Подписывает функцию-обработчик на определенный тип события.

- **Сигнатура:** `(subscribe bus event-type handler & {:keys [schema meta]})`
- **`event-type`:** Ключевое слово (keyword), идентифицирующее событие (например, `:order/created`).
- **`handler`:** Функция, которая будет вызвана. **Важно:** ее сигнатура должна быть `(fn [bus envelope])`, чтобы она могла публиковать производные события.
- **`:schema`:** (опционально) Схема `malli` для валидации полезной нагрузки (`payload`) события. Если валидация не проходит, обработчик не будет вызван.

**Пример:**
```clojure
(bus/subscribe a-bus
               :user/registered
               (fn [bus envelope]
                 (println "New user:" (:payload envelope))
                 ;; Публикация нового события на основе полученного
                 (bus/publish bus
                              :email/send-welcome
                              (:payload envelope)
                              {:parent-envelope envelope
                               :module :mailer})))
```

### `publish`

Публикует событие в шине.

- **Сигнатура:** `(publish bus event-type payload & [opts])`
- **`payload`:** Данные события (обычно карта).
- **`opts`:** (опционально) Карта опций. Внутри требуются `:module` и (опционально) `:schema-version`.
- **Возвращаемое значение:** Функция возвращает созданный "конверт" события (`envelope`). Это карта, содержащая метаданные (`message-id`, `correlation-id` и т.д.) и сами данные (`payload`). Это позволяет инициатору события получить сгенерированный `correlation-id` для дальнейшего отслеживания.

Валидация в `publish` выполняется **строго** по каноническому реестру схем, переданному в `make-bus`.
Если схема отсутствует или payload невалиден, `publish` бросает исключение.

#### Опции для `publish`

- `:parent-envelope`: **Ключевая опция для контроля причинности.** Если вы публикуете событие в ответ на другое, передайте сюда исходный "конверт" (`envelope`). Шина автоматически извлечет `CorrelationID` и обновит `CausationPath`.
- `:module`: **Обязательная опция.** Идентификатор модуля-инициатора (keyword). Используется для контроля циклов по паре `(module, event-type)`.
- `:schema-version`: (опционально) Версия схемы в реестре. По умолчанию `"1.0"`.

**Пример (корневое событие):**
```clojure
;; Кто-то залогинился
(bus/publish a-bus :user/logged-in {:user-id 123} {:module :auth})
```

**Пример (производное событие):**
```clojure
;; Внутри обработчика для :user/logged-in
(fn [bus envelope]
  (let [user-id (-> envelope :payload :user-id)]
    (bus/publish bus
                 :audit/user-activity
                 {:activity "login" :user user-id}
                 {:parent-envelope envelope
                  :module :audit}))) ; <-- Передача родителя и модуля
```

### `transact`

Простой пример транзакционной публикации:

```clojure
(bus/transact a-bus
  [{:event-type :user/created
    :payload {:user-id 42 :email "a@b.com"}
    :module :user}])
```

Подробное описание `transact`, конфигурации `:tx-store` и контрактов обработчиков: [`./TRANSACT.md`](./TRANSACT.md).

### `unsubscribe`

Отписывает обработчик от события.

- **Сигнатура:** `(unsubscribe bus event-type matcher)`
- **`matcher`:** Либо прямая ссылка на функцию-обработчик, либо метаданные (`:meta`), которые были переданы при подписке.

**Пример:**
```clojure
(defn my-handler [bus envelope] (println "Called!"))

(bus/subscribe a-bus :my/event my-handler)

;; ...позже
(bus/unsubscribe a-bus :my/event my-handler)
```

## Envelope (структура сообщения)

`envelope` — карта с метаданными и полезной нагрузкой:

- `:message-id` — UUID сообщения.
- `:correlation-id` — UUID цепочки событий.
- `:causation-path` — вектор пар `[module event-type]`.
- `:message-type` — тип события (keyword).
- `:module` — модуль-инициатор (keyword).
- `:schema-version` — версия схемы (`"1.0"` по умолчанию).
- `:payload` — полезная нагрузка события.

Если событие публикуется с `:parent-envelope`, `correlation-id` сохраняется, а `causation-path` расширяется.

## Ошибки и валидация

- `publish` валидирует `payload` строго по `:schema-registry`. Если схема отсутствует или `payload` невалиден — бросается исключение.
- `:schema` в `subscribe` влияет только на вызов handler. `publish` не зависит от subscriber‑схем.
- Возвращаемое значение handler для обычного `publish` игнорируется (в отличие от `transact`, где нужен `true`).
