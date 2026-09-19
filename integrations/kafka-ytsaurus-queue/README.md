# Kafka + YTsaurus Queue

Связка двух систем, реализующих одну и ту же абстракцию очереди (partition + offset/row index + независимая позиция consumer'а — см. [Kafka: README.md](../../broker/kafka/README.md#ключевые-термины) и [YTsaurus: queues.md](../../big-data/ytsaurus/queues.md#термины)): Kafka закрывает приём событий в реальном времени с низкой задержкой, YTsaurus Queue — дешёвое хранение и batch/ad-hoc обработку тех же данных без выгрузки во внешнее хранилище. Разбирается, какими механизмами они стыкуются и в каких сценариях это оправдано.

## Роли

| Технология | Роль в связке |
|---|---|
| Kafka | приём событий от множества producer'ов в реальном времени, низкая задержка, широкая экосистема клиентов и [Kafka Connect](../../broker/kafka/ecosystem.md)-коннекторов |
| YTsaurus Queue | дешёвое хранение больших объёмов данных (включая долгосрочное — export в static table), доступ к тем же данным через batch-вычисления (Map/Reduce) и ad-hoc SQL ([CHYT/YQL](../../big-data/ytsaurus/ecosystem.md)) без отдельного ETL во внешнее хранилище |

## Механизмы интеграции

- **Kafka Connect sink-коннектор** ([kafka-connect-ytsaurus](https://github.com/Dzen-Platform/kafka-connect-ytsaurus), класс `ru.dzen.kafka.connect.ytsaurus.YtTableSinkConnector`) — читает Kafka-топик и пишет в YTsaurus. Выходная таблица — [dynamic table](../../big-data/ytsaurus/dynamic-tables.md) (по умолчанию, данные попадают в [queue](../../big-data/ytsaurus/queues.md#термины)) либо static table с ротацией по времени (`yt.sink.output.type`). Формат записи значения (`yt.sink.output.value.format`): `strict` (по Kafka Connect-схеме) и `weak` (схема инферится из JSON) несовместимы с dynamic table — для записи в queue нужен `unstructured` (сырые данные + метаданные в отдельные колонки). Офсеты Kafka-топика сохраняются в YTsaurus, что даёт exactly-once доставку при записи. `yt.sink.output.ttl` (строка вида `30d`) включает авто-удаление данных по TTL.
- **Kafka Proxy** — компонент YTsaurus, делает queue протокол-совместимой с Apache Kafka напрямую, без коннектора: клиенты пишут/читают queue штатным Kafka SDK. На текущей стадии (MVP) поддерживает запись и чтение с балансировкой через consumer group; per-consumer offset storage и несколько proxy одновременно — в более поздних релизах по [roadmap YTsaurus](https://github.com/ytsaurus/ytsaurus/wiki/YTsaurus-Roadmap-draft-03.2025). Перед тем как полагаться на Kafka Proxy в проде — сверяться с актуальным roadmap/release notes, набор возможностей активно расширяется.
- **Export очереди в static table** — встроенная возможность YTsaurus queue выгружать сообщения в статическую таблицу для долгосрочного хранения, независимо от того, как данные попали в queue (коннектор или Kafka Proxy).
- **Custom reader + Kafka producer (обратное направление, YTsaurus → Kafka)** — официального source-коннектора YTsaurus → Kafka в экосистеме нет. Обратный поток реализуется вручную: собственный сервис читает YTsaurus queue штатным [Consumer API](../../big-data/ytsaurus/queues.md#термины) (`pull_consumer`/`advance_consumer`) и публикует прочитанное в Kafka обычным producer'ом. На этом building block построены юзкейсы 5–7 ниже.

## Юзкейсы

Сводно, какую роль в каждом сценарии играет каждая технология:

| Юзкейс | Роль Kafka | Роль YTsaurus Queue |
|---|---|---|
| 1. Ingestion → Stateful | приём сырого потока (ingest) | буфер отфильтрованных задач с retry/replay |
| 2. Долгосрочный replay | краткосрочный low-latency буфер | дешёвый архив + batch compute для replay |
| 3. Fan-out на классы потребителей | real-time consumer (низкая задержка) | batch/CHYT consumer (независимая позиция) |
| 4. Миграция клиентов | легаси-протокол, из которого уходят | целевая система, принимающая тех же клиентов через Kafka Proxy |
| 5. CDC / Outbox | fan-out-раздача подписчикам | источник правды (ACID) + отправная точка CDC |
| 6. Micro-batching | транспорт онлайн-данных | накопительный буфер батчей |
| 7. Feedback loop | шина телеметрии и логов (observe) | оркестратор сложных задач (act) |

### 1. Ingestion: "быстрый тупой + медленный умный" — Kafka глотает весь поток, YTsaurus считает аналитику на отфильтрованном срезе

Firehose событий (миллионы/сек) целиком идёт в Kafka — она справляется с таким объёмом без предварительной обработки за счёт sequential I/O + zero-copy (append-only лог, см. Плюсы в [kafka/README.md](../../broker/kafka/README.md)): это "быстрый тупой" слой, который просто дописывает всё подряд. Для аналитики нужна не вся история, а узкая выборка (например ~5% потока) — фильтрация делается ещё на стороне Kafka, до записи в YTsaurus: ksqlDB-запрос (см. [ecosystem.md](../../broker/kafka/ecosystem.md)) читает сырой топик и пишет отфильтрованный поток в отдельный топик, который уже забирает sink-коннектор. В YTsaurus этот узкий срез попадает в queue как [распределённая транзакция с полной atomicity](../../big-data/ytsaurus/dynamic-tables.md#термины) (режим по умолчанию `full`) — это "медленный умный" слой: аналитика (join, агрегации через Map/Reduce/CHYT/YQL) строится на согласованных данных, без риска увидеть частично применённую запись, чего сырой Kafka-топик сам по себе не гарантирует. Так как очередь хранит строки по [row index](../../big-data/ytsaurus/queues.md#термины) и не удаляет их сразу после чтения, тот же consumer при сбое обработки может просто не вызывать `advance_consumer` и перечитать тот же батч заново — очередь попутно работает как надёжный буфер отложенных задач с retry, а не только как staging для аналитики.

```sql
-- ksqlDB: оставляем только события нужного типа из сырого firehose-топика raw_events
CREATE STREAM purchase_events AS
  SELECT * FROM raw_events
  WHERE event_type = 'purchase'
  EMIT CHANGES;
```

```json
{
  "name": "purchase-events-to-ytsaurus",
  "config": {
    "connector.class": "ru.dzen.kafka.connect.ytsaurus.YtTableSinkConnector",
    "topics": "purchase_events",
    "yt.connection.cluster": "my-cluster.yt.company.com",
    "yt.connection.user": "kafka-connect",
    "yt.connection.token": "${file:/secrets/yt-token.properties:token}",
    "yt.sink.output.type": "dynamic",
    "yt.sink.output.value.format": "unstructured",
    "yt.sink.output.directory": "//home/project/kafka-import",
    "yt.sink.output.ttl": "30d"
  }
}
```

### 2. Дешёвый долгосрочный replay истории, которую Kafka не может хранить экономически

Нужен replay событий за месяцы/годы (например переобучение ML-модели на полной истории), но держать всё это время в Kafka дорого — retention там ограничен стоимостью хранения на брокерах. Kafka остаётся краткосрочным буфером с низкой задержкой; sink-коннектор параллельно архивирует те же события в YTsaurus (queue → export в static table), где хранение дешевле за счёт erasure coding (см. [architecture.md](../../big-data/ytsaurus/architecture.md)) и есть полноценный batch compute для переобработки всей истории целиком — того, что Kafka сама по себе не даёт.

### 3. Fan-out на разные классы потребителей без взаимного влияния

Одни и те же события нужны real-time сервису (низкая задержка, обработка по одному событию) и batch/аналитическому job'у (обрабатывает большими пачками, может сильно отставать). Оба читателя используют независимую модель consumer со своей позицией: real-time сервис читает Kafka через consumer group напрямую, batch/CHYT-job — YTsaurus queue (куда события попадают через sink-коннектор) через свой [YTsaurus consumer](../../big-data/ytsaurus/queues.md#термины). Медленный batch-consumer не создаёт back-pressure на Kafka и не блокирует real-time consumer, потому что физически читает из другого хранилища.

```
Producers ──► Kafka topic ──► real-time service (Kafka consumer group, низкая задержка)
                  │
                  └─(sink-коннектор)─► YTsaurus queue ──► batch/CHYT job (YTsaurus consumer, отстаёт без вреда для real-time)
```

### 4. Инкрементальная миграция клиентов с Kafka на YTsaurus Queue

Нужно постепенно переносить producer'ов/consumer'ов на YTsaurus, не переписывая все клиенты одновременно. Kafka Proxy делает queue протокол-совместимой с Kafka: существующие Kafka-клиенты продолжают работать (запись + load-balanced чтение через consumer group) против YTsaurus, без коннектора и без промежуточного топика — сервисы переключаются по одному. Ограничение текущего MVP (см. «Механизмы интеграции») — закладывать при выборе этого пути для прод-нагрузки.

### 5. CDC / Outbox поверх YTsaurus Queue: YTsaurus как источник правды, Kafka — раздатчик подписчикам

Нужно синхронизировать основное состояние (например состояние заказа) с десятками независимых систем-подписчиков (поисковый индекс, аналитические витрины, кэши, ML-платформы), не теряя данные при сбоях и не нагружая основную запись прямыми обращениями от каждого подписчика.

Сервис пишет данные напрямую в YTsaurus queue — запись идёт как [распределённая транзакция с atomicity=full](../../big-data/ytsaurus/dynamic-tables.md#термины), поэтому запись либо целиком применяется и видна, либо не видна вовсе. По сути это [паттерн outbox](../../broker/kafka/delivery-semantics.md#термины) без отдельной outbox-таблицы поверх внешней БД: сама очередь и есть одновременно источник правды и лог событий. CDC-reader — сервис из «Механизмов интеграции» (обычный YTsaurus consumer) — вычитывает эту очередь и публикует события в Kafka-топик. Дальше Kafka раздаёт событие любому числу независимых подписчиков — каждый читает топик своей consumer group (см. [README.md](../../broker/kafka/README.md#ключевые-термины)), не создавая нагрузки друг на друга и на исходный сервис.

```
Service ──(tx, atomicity=full)──► YTsaurus Queue (источник правды)
                                          │ (YTsaurus consumer: pull/advance)
                                          ▼
                                    CDC-reader service ──► Kafka topic ──► Consumer group "search"
                                                                       ├─► Consumer group "analytics"
                                                                       └─► Consumer group "ml-platform"
```

Почему нужна именно связка: YTsaurus даёт транзакционную надёжность записи (данные не теряются при падении сервиса), Kafka — дешёвую раздачу одного потока сразу многим независимым потребителям, которую пришлось бы иначе реализовывать десятками прямых интеграций с основной БД.

### 6. Micro-batching: сборка крупных батчей для тяжёлой Analytics/ML обработки

OLAP-запросы и обучение ML-моделей эффективны на крупных батчах, а не на потоке мелких событий по одному — запись каждого события напрямую в аналитическое хранилище создаёт слишком много мелких транзакций/чанков.

Поток событий постоянно льётся в Kafka. Коллектор читает Kafka и пишет события в YTsaurus queue. Очередь накапливает данные, и воркер забирает их не по одному событию, а батчами — например каждые 5 минут или по накоплению 100 000 строк (`pull_consumer` c `max_row_count=100000`, см. [queues.md](../../big-data/ytsaurus/queues.md#примеры-использования)). Забрав батч, воркер одной тяжёлой транзакцией вливает его в static table (вход для Map/Reduce/обучения модели) и продвигает позицию (`advance_consumer`) только после успешной записи.

Почему нужна именно связка: Kafka — дешёвый по задержке транспорт непрерывного потока, но сама по себе не даёт batch compute и не должна использоваться как буфер накопления для тяжёлых batch-job'ов (см. Минусы в [kafka/README.md](../../broker/kafka/README.md)). YTsaurus queue выступает staging area: [atomicity=full](../../big-data/ytsaurus/dynamic-tables.md#термины) гарантирует, что батч соберётся полностью и ровно один раз, прежде чем попадёт в тяжёлое хранилище.

### 7. Feedback loop: Kafka наблюдает, YTsaurus Queue управляет

Нужно автоматически реагировать на инциденты инфраструктуры многошаговым планом (перезапуск, прогрев кэшей, health-check) и гарантированно довести план до конца, даже если воркер, выполняющий шаги, упадёт посередине.

Логи, метрики и ошибки сервисов текут через Kafka в сервис-анализатор — Kafka здесь чистая шина телеметрии и наблюдаемости (observe). Обнаружив проблему, анализатор создаёт в YTsaurus queue долгоживущий граф задач — упорядоченный по [row index](../../big-data/ytsaurus/queues.md#термины) список шагов runbook'а. Воркеры читают задачи как обычный YTsaurus consumer и продвигают позицию (`advance_consumer`) только после успешного выполнения шага — если воркер упал посередине, при перезапуске он получит тот же незавершённый шаг заново (at-least-once, см. [queues.md](../../big-data/ytsaurus/queues.md)). Результаты выполнения публикуются обратно в Kafka, чтобы мониторинг увидел закрытие инцидента.

Почему нужна именно связка: Kafka закрывает роль шины телеметрии (много источников, много подписчиков, высокий throughput), YTsaurus queue — роль оркестратора (act): гарантированное, упорядоченное исполнение управляющих команд с сохранением прогресса при падении воркера, чего у Kafka из коробки нет (consumer сам решает, что считать «выполненным», а не система очереди).

## Особенности/trade-offs

- Официального source-коннектора YTsaurus → Kafka нет (см. «Механизмы интеграции») — весь egress-трафик (юзкейсы 5–7) держится на собственном сервисе-читателе, а не на готовом продукте с гарантиями из коробки.
- Kafka Proxy и sink-коннектор — независимые механизмы с разными гарантиями (exactly-once через сохранённые офсеты у коннектора против MVP-возможностей Kafka Proxy) — не смешивать в одном pipeline без проверки, что это даёт нужные гарантии доставки.
