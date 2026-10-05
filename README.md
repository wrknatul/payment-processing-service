# Сервис процессинга платежей

Принимает платёж по HTTP, обрабатывает его в фоне через платёжный шлюз (эмуляция) и сообщает
клиенту результат webhook'ом.

Стек: FastAPI, Pydantic v2, SQLAlchemy 2.0 (async, asyncpg), PostgreSQL, RabbitMQ через FastStream,
Alembic, Docker Compose.

## Как это работает

```
клиент
  │  POST /api/v1/payments
  ▼
 api
  │  одна транзакция: INSERT в payments и в outbox (PostgreSQL)
  ▼
outbox-relay
  │  SELECT ... FOR UPDATE SKIP LOCKED, publish с подтверждением брокера
  ▼
exchange payments ──▶ очередь payments.new ──▶ consumer: шлюз → статус в БД → webhook
                             ▲                     │
                             │ TTL истёк           │ ошибка, попытки ещё есть
               очереди payments.retry.<мс> ◀───────┤
                                                   │ 3-я ошибка или битое сообщение
                                                   ▼
                                        exchange payments.dlx ──▶ очередь payments.dlq
```

Из одного образа запускаются три процесса:

| Сервис | Что делает |
| --- | --- |
| `api` | HTTP API. Пишет только в PostgreSQL и о RabbitMQ не знает, поэтому принимает платежи и при недоступном брокере |
| `outbox-relay` | Переносит события из таблицы `outbox` в RabbitMQ |
| `consumer` | Единственный обработчик очереди `payments.new`: шлюз, статус, webhook, retry |

Ещё в compose: `postgres`, `rabbitmq`, `migrate` (разово накатывает миграции) и `webhook-sink` —
демонстрационный приёмник webhook'ов.

## Запуск

```bash
docker compose up --build
```

Файл `.env` не нужен. После старта:

- API и Swagger: http://localhost:8000/docs
- RabbitMQ Management: http://localhost:15672 (guest / guest)
- принятые webhook'и: http://localhost:8080/received

Если порт на хосте занят, его можно переназначить, например
`API_PORT=18000 POSTGRES_PORT=15432 docker compose up --build`. Все переменные перечислены в
`.env.example`.

## Примеры

Создать платёж:

```bash
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H 'X-API-Key: dev-api-key' \
  -H 'Idempotency-Key: order-42' \
  -H 'Content-Type: application/json' \
  -d '{
        "amount": "1500.00",
        "currency": "RUB",
        "description": "Заказ 42",
        "metadata": {"order_id": "42"},
        "webhook_url": "http://webhook-sink:8080/ok"
      }'
```

```
HTTP/1.1 202 Accepted
x-request-id: ad33e396-599d-4096-8cd6-7931a952cc93

{"payment_id":"c10ab690-49f4-48fa-9246-7a87452cd852","status":"pending","created_at":"2026-10-05T14:25:53.013432Z"}
```

`webhook-sink` в адресе — имя сервиса в сети compose: webhook отправляет consumer изнутри этой
сети, поэтому `localhost` здесь не подойдёт.

Тот же запрос ещё раз возвращает тот же платёж и заголовок `Idempotent-Replayed: true`; второй
платёж и второе событие не создаются.

Тот же `Idempotency-Key` с другим телом:

```
HTTP/1.1 409 Conflict

{"success":false,"error_code":"IDEMPOTENCY_KEY_CONFLICT","message":"Idempotency-Key was already used with a different request body","trace_id":"7204ce5b-6ea6-435a-a11d-e4e79564a71a","details":{}}
```

Получить платёж:

```bash
curl http://localhost:8000/api/v1/payments/c10ab690-49f4-48fa-9246-7a87452cd852 \
  -H 'X-API-Key: dev-api-key'
```

```json
{
  "payment_id": "c10ab690-49f4-48fa-9246-7a87452cd852",
  "amount": "1500.00",
  "currency": "RUB",
  "description": "Заказ 42",
  "metadata": {"order_id": "42"},
  "status": "succeeded",
  "idempotency_key": "order-42",
  "webhook_url": "http://webhook-sink:8080/ok",
  "webhook_delivered_at": "2026-10-05T14:25:57.420883Z",
  "created_at": "2026-10-05T14:25:53.013432Z",
  "processed_at": "2026-10-05T14:25:57.396278Z"
}
```

Без ключа или с неверным ключом любой эндпоинт `/api/v1/*` отвечает `401`:

```json
{"success":false,"error_code":"INVALID_API_KEY","message":"Missing or invalid API key","trace_id":"92f60ef3-dfe3-4e94-8504-80d1a4db03c2","details":{}}
```

Все ошибки приходят в этом формате. `trace_id` берётся из заголовка запроса `X-Request-ID` или
генерируется и возвращается в том же заголовке ответа.

| Код | `error_code` | Когда |
| --- | --- | --- |
| 401 | `INVALID_API_KEY` | нет ключа или он неверный |
| 404 | `PAYMENT_NOT_FOUND` | платежа с таким id нет |
| 409 | `IDEMPOTENCY_KEY_CONFLICT` | ключ уже использован с другим телом запроса |
| 422 | `VALIDATION_ERROR` | невалидное тело или нет `Idempotency-Key`; подробности в `details.errors` |
| 503 | `SERVICE_UNAVAILABLE` | недоступна база данных; запрос можно повторить с тем же ключом |
| 500 | `INTERNAL_ERROR` | непредвиденная ошибка; подробности только в логах, по `trace_id` |

Webhook, который получает клиент:

```
POST /ok
Content-Type: application/json
X-Webhook-Event-Id: c10ab690-49f4-48fa-9246-7a87452cd852
X-Webhook-Signature: sha256=<HMAC-SHA256 тела на WEBHOOK_SIGNING_SECRET>

{"event_id":"c10ab690-...","event_type":"payment.succeeded","payment_id":"c10ab690-...","status":"succeeded","amount":"1500.00","currency":"RUB","description":"Заказ 42","metadata":{"order_id":"42"},"created_at":"2026-10-05T14:25:53.013432Z","processed_at":"2026-10-05T14:25:57.396278Z"}
```

## Как посмотреть retry и DLQ

У демонстрационного приёмника три адреса: `/ok` всегда отвечает 200, `/fail` всегда 500, `/flaky`
отвечает 500 на первые две доставки события и 200 на третью.

Платёж с `"webhook_url": "http://webhook-sink:8080/flaky"` доставляется с третьей попытки, через
5 и 10 секунд после неудач. В логах consumer'а (`docker compose logs -f consumer`):

```
payment 61e3b90d-...: succeeded
payment 61e3b90d-...: attempt 1/3 failed, retrying in 5000 ms: webhook to http://webhook-sink:8080/flaky answered HTTP 500
payment 61e3b90d-...: attempt 2/3 failed, retrying in 10000 ms: webhook to http://webhook-sink:8080/flaky answered HTTP 500
payment 61e3b90d-...: webhook delivered
```

Шлюз при этом вызывается один раз: повторные попытки видят финальный статус платежа и только
повторяют отправку webhook'а.

Платёж с `"webhook_url": "http://webhook-sink:8080/fail"` после трёх попыток уходит в
`payments.dlq`:

```bash
docker compose exec rabbitmq rabbitmqctl -q list_queues name messages
```

```
name	messages
payments.new	0
payments.retry.10000	0
payments.retry.5000	0
payments.dlq	1
```

У сообщения в DLQ сохраняются заголовки `x-attempt: 3` и `x-last-error` с текстом последней ошибки,
а RabbitMQ добавляет `x-death` с причиной `rejected`.

## Решения и гарантии

### Outbox

Платёж и событие `payment.created` записываются одной транзакцией, так что события без платежа или
платежа без события не бывает. Публикацией занимается отдельный процесс `outbox-relay`:

- выбирает неопубликованные строки `FOR UPDATE SKIP LOCKED` — несколько экземпляров relay могут
  работать параллельно и не публикуют одну строку дважды;
- публикует с publisher confirms, флагом `mandatory` и `delivery_mode=persistent`; если брокер не
  подтвердил сообщение или не смог положить его в очередь, это ошибка, а не тихая потеря;
- ставит `published_at` только после подтверждения, в той же транзакции, которая держит блокировку;
- при ошибке оставляет строку неопубликованной, пишет `attempts` и `last_error` и повторяет с
  растущей паузой (до 30 секунд).

Гарантия доставки — at-least-once: если relay упадёт между подтверждением брокера и коммитом,
событие будет опубликовано повторно. Поэтому consumer идемпотентен.

### Идемпотентность создания платежа

`INSERT ... ON CONFLICT (idempotency_key) DO NOTHING RETURNING`: одновременные запросы с одним
ключом разводит уникальный индекс, вставляет ровно один из них. Остальные дожидаются его коммита,
читают существующий платёж и возвращают его. Схема «сначала SELECT, потом INSERT» пропустила бы оба
запроса.

Вместе с платежом хранится хэш тела запроса. Повтор с тем же телом — это `202` и тот же платёж, тот
же ключ с другим телом — `409`. Сумма перед хэшированием нормализуется, поэтому `1500` и `1500.00`
считаются одним запросом.

### Идемпотентность consumer'а

Сообщение может прийти повторно: из outbox, при переподключении к брокеру, при retry. Обработчик
опирается на состояние платежа в БД, а не на то, видел ли он сообщение:

- платёж уже в финальном статусе — шлюз повторно не вызывается;
- статус меняется запросом `UPDATE ... WHERE status = 'pending'`: если два экземпляра обработают
  одно сообщение одновременно, результат запишет только первый, второй возьмёт его результат;
- webhook уже доставлен (`webhook_delivered_at`) — повторно не отправляется.

Вызов шлюза выполняется вне транзакции БД, чтобы не держать соединение и блокировки на время
внешнего запроса. Обратная сторона: два одновременно обрабатываемых дубля оба дойдут до шлюза.
Эмулятору это безразлично; с настоящим шлюзом это закрывается ключом идемпотентности в запросе к
нему (id платежа), тогда повторный вызов не приводит ко второму списанию.

### «10 % ошибок» шлюза

Отказ шлюза трактуется как результат обработки: платёж получает статус `failed`, клиент получает
webhook `payment.failed`. Это финальный статус, повторять списание после отказа нельзя. Retry
относится к сбоям обработки: недоступен webhook клиента, БД или брокер.

### RabbitMQ

| Exchange (direct) | Routing key | Очередь | Аргументы очереди |
| --- | --- | --- | --- |
| `payments` | `payments.new` | `payments.new` | `x-dead-letter-exchange=payments.dlx`, `x-dead-letter-routing-key=payments.dead` |
| `payments.retry` | `payments.retry.<мс>` | `payments.retry.<мс>` | `x-message-ttl=<мс>`, dead-letter обратно в `payments` / `payments.new` |
| `payments.dlx` | `payments.dead` | `payments.dlq` | — |

Все exchange и очереди durable, сообщения persistent. Топологию при старте объявляют и relay, и
consumer, поэтому порядок их запуска не важен.

### Retry

Попыток всего три. После неудачной попытки `n` сообщение публикуется в очередь с TTL
`RETRY_BASE_DELAY_SECONDS * RETRY_BACKOFF_MULTIPLIER^(n-1)` — по умолчанию 5 и 10 секунд. По
истечении TTL брокер сам возвращает его в `payments.new`. Номер попытки едет в заголовке
`x-attempt`.

- Задержка живёт в брокере, а не в `sleep` внутри обработчика: consumer не блокируется и ожидающие
  сообщения переживают его перезапуск.
- На каждую задержку своя очередь: в одной очереди с TTL на сообщении короткая задержка ждала бы
  за длинной, стоящей в голове.
- TTL входит в имя очереди. Очередь нельзя переобъявить с другими аргументами, поэтому при смене
  настроек создаются новые очереди, а не ошибка `PRECONDITION_FAILED`.
- Копия для retry публикуется с подтверждением до ack оригинала. Сбой между этими шагами даёт
  дубль, но не потерю.

### Dead Letter Queue

В `payments.dlq` попадают сообщения, которые не удалось обработать за три попытки, сообщения с
неисправимой ошибкой (платёж не найден) и сообщения, которые не разбираются в событие — они уходят
туда сразу, без retry. Туда же уходит сообщение, для которого не удалось опубликовать копию на
retry: в DLQ оно сохранится для ручного повтора, а возврат в очередь дал бы бесконечную
мгновенную повторную доставку, пока брокер не принимает публикации.

Используется штатный механизм брокера: consumer отклоняет сообщение без requeue, очередь
`payments.new` отправляет его в `payments.dlx`.

Что видно по платежу, сообщение которого оказалось в DLQ:

- не доставлен webhook — статус уже финальный и доступен через `GET`, `webhook_delivered_at`
  пустой;
- сбой случился раньше записи статуса (например, база была недоступна дольше, чем длятся три
  попытки) — платёж остаётся `pending`, пока сообщение не вернут из DLQ в `payments.new`.

### Webhook

Тело подписывается HMAC-SHA256 (`X-Webhook-Signature`). Доставка at-least-once, получатель
отсекает дубли по `X-Webhook-Event-Id`. Успехом считается только ответ 2xx; редиректы не
выполняются.

### API-ключ

`X-API-Key` обязателен для всех эндпоинтов `/api/v1/*`, сравнение за постоянное время. `GET /health`
открыт намеренно: его опрашивает healthcheck Docker.

## Конфигурация

| Переменная | По умолчанию | Назначение |
| --- | --- | --- |
| `DATABASE_URL` | `postgresql+asyncpg://payments:payments@localhost:5432/payments` | PostgreSQL |
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:5672/` | RabbitMQ |
| `API_KEY` | `dev-api-key` | Статический ключ API |
| `WEBHOOK_SIGNING_SECRET` | `dev-webhook-secret` | Секрет подписи webhook'ов |
| `WEBHOOK_TIMEOUT_SECONDS` | `5` | Таймаут запроса к webhook'у |
| `MAX_ATTEMPTS` | `3` | Всего попыток обработки сообщения |
| `RETRY_BASE_DELAY_SECONDS` | `5` | Задержка перед второй попыткой |
| `RETRY_BACKOFF_MULTIPLIER` | `2` | Множитель задержки |
| `CONSUMER_PREFETCH` | `10` | Сколько сообщений consumer обрабатывает одновременно |
| `OUTBOX_POLL_INTERVAL_SECONDS` | `1` | Период опроса outbox |
| `OUTBOX_BATCH_SIZE` | `100` | Размер пачки relay |
| `GATEWAY_MIN_DELAY_SECONDS` / `GATEWAY_MAX_DELAY_SECONDS` | `2` / `5` | Время ответа эмулятора шлюза |
| `GATEWAY_SUCCESS_RATE` | `0.9` | Доля успешных платежей |
| `LOG_LEVEL` | `INFO` | Уровень логов |

## Тесты

```bash
docker compose --profile test run --rm --build tests
```

Интеграционные тесты работают с настоящими PostgreSQL и RabbitMQ из compose: используют отдельную
базу `payments_test` и очереди с уникальным префиксом, поэтому не мешают запущенному сервису.

Локально:

```bash
uv sync
uv run pytest tests/unit                      # без инфраструктуры
TEST_DATABASE_URL=postgresql+asyncpg://payments:payments@localhost:5432/payments_test \
TEST_RABBITMQ_URL=amqp://guest:guest@localhost:5672/ \
uv run pytest                                 # всё, нужны запущенные postgres и rabbitmq
make lint                                     # ruff и mypy --strict
```

Что проверяется:

- API на реальной БД: создание, повтор, конфликт ключа, 20 одновременных запросов с одним ключом
  (один платёж и одно событие), аутентификация, формат ошибок;
- relay: порядок публикации, сбой публикации посреди пачки, два параллельных relay;
- retry и DLQ на реальном брокере: три попытки с растущей задержкой, восстановление на повторе,
  неисправимая ошибка, битые сообщения, неподтверждённая публикация;
- логика процессора: шлюз не вызывается повторно, результат конкурирующего дубля не
  перезаписывается.

## Структура

```
app/
  api/          HTTP: приложение, зависимости, схемы, формат ошибок, trace id
  services/     создание платежа (payments), обработка (processor), шлюз, webhook
  messaging/    топология RabbitMQ, политика retry, обработчик, consumer (FastStream)
  outbox/       relay: outbox → RabbitMQ
  db/           модели, сессии, репозиторий consumer'а
alembic/        миграции
demo/           демонстрационный приёмник webhook'ов
tests/          unit и integration
```

## Что осталось за рамками

- Проверка `webhook_url` на внутренние адреса (SSRF).
- Очистка опубликованных строк `outbox`.
- Метрики и алерты: длина DLQ, отставание outbox, доля неуспешных webhook'ов.
- Повторная обработка сообщений из DLQ.
- Quorum-очереди для кластера RabbitMQ.
- Отдельные API-ключи и секреты подписи на каждого клиента, их ротация.
