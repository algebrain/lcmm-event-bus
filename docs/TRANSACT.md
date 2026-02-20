# TRANSACT

Этот документ описывает `transact` — транзакционную публикацию событий через внутреннюю БД.

## Термины

- **Внешняя БД** — основная бизнес‑база приложения (источник бизнес‑данных).
- **Внутренняя БД** — база, в которой `event-bus` хранит состояние транзакций и прогресс обработки сообщений `transact`.

## Основная идея

Главный принцип: **ID новой записи формируется до записи во внешнюю БД**, а все операции во внешней БД выполняются в обработчике `transact` основного модуля. То есть модуль, принявший API‑вызов, **подписывается на собственное сообщение** и фиксирует бизнес‑данные уже в своём обработчике.

Это позволяет:
- не держать внешнюю БД в частично‑записанном состоянии,
- сделать публикацию событий источником истины для последовательности действий,
- гарантировать, что запись во внешнюю БД происходит только после корректной обработки всех сообщений `transact`.

Важно: **регистрация пользователя — лишь иллюстрация**. `transact` — общий механизм для любых операций, где нужны воспроизводимость и полная обработка цепочки событий.

## Контракт и гарантии

1. Сообщение `transact` считается обработанным только после того, как **все его обработчики вернули `true`**.
2. Обработчики `transact` **обязаны быть идемпотентными** (возможна повторная обработка после сбоев).
3. Состояние транзакции хранится во внутренней БД — это рабочие данные, а не лог.

## Внутренняя БД

По умолчанию используется **Datahike** с backend `:file`. Пользователь библиотеки может выбрать другой backend через опции.

Пример конфигурации Datahike:

```clojure
{:db/type :datahike
 :datahike/config {:store {:backend :file
                           :path "./data/event-bus"}
                   :schema-flexibility :write}}
```

## API

### make-bus

```clojure
(bus/make-bus
  :schema-registry registry
  :tx-store {:datahike/config {:store {:backend :file
                                       :path "./data/event-bus"}
                               :schema-flexibility :write}}
  :tx-handler-timeout 10000
  :handler-max-retries 3
  :handler-backoff-ms 1000)
```

Если `:tx-store` не указан, `transact` бросает исключение.
Если `:db/type` не указан, по умолчанию используется `:datahike`.

### transact

```clojure
(transact bus
  [{:event-type :user/created
    :payload {:user-id 42 :email "a@b.com"}
    :module :user}
   {:event-type :audit/user-created
    :payload {:user-id 42}
    :module :audit}])
```

`transact`:
- атомарно фиксирует список сообщений во внутренней БД;
- возвращает `:op-id`, `:result-promise` и `:result-chan`.

### Результат

Успех: все обработчики всех сообщений вернули `true`.
Ошибка: хотя бы один обработчик вернул `false`, выбросил исключение или превысил таймаут.

## Использование (синхронно)

```clojure
(let [{:keys [result-promise]} (transact bus events)
      result (deref result-promise 5000 ::timeout)]
  (cond
    (= result ::timeout) {:status 202 :body "processing"}
    (:ok? result)        {:status 200 :body (:payload result)}
    :else                {:status 500 :body (:error result)}))
```

## Использование (асинхронно через channel)

```clojure
(let [{:keys [op-id result-chan]} (transact bus events)]
  (async/go
    (when-let [result (async/<! result-chan)]
      ;; здесь отправляем результат в WebSocket
      (send-ws! op-id result)))
  {:status 202 :body {:operation op-id}})
```

## Рекомендации

- Для операций, где нужна строгая согласованность между модулями — используйте `transact`.
- Для “мягких” уведомлений и некритичных действий — используйте `publish`.

## Интеграция в реальный API (пример)

### 1. HTTP → transact → ожидание результата

```clojure
(defn register-handler [req]
  (let [user-id (UUID/randomUUID)
        events [{:event-type :user/created
                 :payload {:user-id user-id :email (get-in req [:params :email])}
                 :module :user}]
        {:keys [result-promise]} (transact bus events)
        result (deref result-promise 5000 ::timeout)]
    (cond
      (= result ::timeout) {:status 202 :body {:operation user-id}}
      (:ok? result)        {:status 200 :body {:user-id user-id}}
      :else                {:status 500 :body (:error result)})))
```

### 2. HTTP → transact → WebSocket уведомление

```clojure
(defn register-handler-async [req]
  (let [user-id (UUID/randomUUID)
        events [{:event-type :user/created
                 :payload {:user-id user-id :email (get-in req [:params :email])}
                 :module :user}]
        {:keys [op-id result-chan]} (transact bus events)]
    (async/go
      (when-let [result (async/<! result-chan)]
        (send-ws! op-id result)))
    {:status 202 :body {:operation op-id}}))
```

---
