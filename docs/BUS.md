# Документация по API: event-bus

Этот документ описывает публичный API для `event-bus`.

## Конструктор

### `make-bus`

Создает новый экземпляр шины.

```clojure
(require '[event-bus :as bus])

(def a-bus (bus/make-bus))
```

#### Опции

Вы можете передать карту опций для конфигурации:

- `:mode`: Режим работы.
  - `:unlimited` (по умолчанию): Каждый обработчик выполняется в новом виртуальном потоке.
  - `:buffered`: События помещаются в очередь фиксированного размера, из которой их забирает ограниченный пул потоков.
- `:max-depth`: Максимальная глубина цепочки событий (по умолчанию: `20`).
- `:logger`: Функция для логирования, принимающая `(fn [level data])`.
- `:buffer-size`: (для режима `:buffered`) Размер очереди (по умолчанию: `1024`).
- `:concurrency`: (для режима `:buffered`) Количество потоков-обработчиков (по умолчанию: `4`).

**Пример с опциями:**
```clojure
(def buffered-bus
  (bus/make-bus {:mode :buffered
                 :buffer-size 500
                 :concurrency 8
                 :logger (fn [lvl d] (println "LOG:" lvl d))}))
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
                              {:parent-envelope envelope})))
```

### `publish`

Публикует событие в шине.

- **Сигнатура:** `(publish bus event-type payload & [opts])`
- **`payload`:** Данные события (обычно карта).
- **`opts`:** (опционально) Карта опций.
- **Возвращаемое значение:** Функция возвращает созданный "конверт" события (`envelope`). Это карта, содержащая метаданные (`message-id`, `correlation-id` и т.д.) и сами данные (`payload`). Это позволяет инициатору события получить сгенерированный `correlation-id` для дальнейшего отслеживания.

#### Опции для `publish`

- `:parent-envelope`: **Ключевая опция для контроля причинности.** Если вы публикуете событие в ответ на другое, передайте сюда исходный "конверт" (`envelope`). Шина автоматически извлечет `CorrelationID` и обновит `CausationPath`.

**Пример (корневое событие):**
```clojure
;; Кто-то залогинился
(bus/publish a-bus :user/logged-in {:user-id 123})
```

**Пример (производное событие):**
```clojure
;; Внутри обработчика для :user/logged-in
(fn [bus envelope]
  (let [user-id (-> envelope :payload :user-id)]
    (bus/publish bus
                 :audit/user-activity
                 {:activity "login" :user user-id}
                 {:parent-envelope envelope}))) ; <-- Передача родителя
```

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