# Kafka

Распределённый брокер сообщений (event streaming platform). Модель — publish/subscribe с журналом (log), а не очередь: сообщения не удаляются после чтения, хранятся заданное время/объём.

## Ключевые термины

- **Cluster** — группа брокеров, работающих вместе как единая система. Топики и их партиции распределены по брокерам кластера; клиент подключается не к одному брокеру, а к кластеру целиком (через `bootstrap.servers` — список из нескольких брокеров для первоначального подключения и обнаружения остальных).
- **Broker** — один сервер (узел) Kafka-кластера. Хранит часть партиций (как leader и/или follower), обслуживает запросы producer'ов и consumer'ов на чтение/запись.
- **Topic** — именованный поток событий, логическая категория сообщений. Аналог "таблицы" или "очереди".
- **Partition** — топик делится на партиции. Каждая партиция — упорядоченный, неизменяемый (append-only) лог. Порядок сообщений гарантирован только внутри партиции, не в топике целиком. Подробнее об устройстве и репликации партиций — в [partitions-and-replication.md](partitions-and-replication.md).
- **Offset** — порядковый номер сообщения внутри партиции. Consumer хранит свой offset, чтобы знать, что уже прочитано.
- **Producer** — пишет сообщения в топик (в конкретную партицию, по ключу или round-robin).
- **Consumer** — читает сообщения из партиций.
- **Consumer Group** — группа consumer'ов, которые вместе читают топик; каждая партиция читается только одним consumer'ом из группы одновременно (это даёт горизонтальное масштабирование чтения).

## Быстрый старт

```bash
# создать топик с 3 партициями и репликацией 2
kafka-topics.sh --create --topic orders --partitions 3 --replication-factor 2 --bootstrap-server localhost:9092

# отправить сообщение
kafka-console-producer.sh --topic orders --bootstrap-server localhost:9092

# прочитать всё с начала
kafka-console-consumer.sh --topic orders --from-beginning --bootstrap-server localhost:9092
```

Подробнее про флаги и режимы этих консольных утилит — в [console-tools.md](console-tools.md).

## Разделы

- [console-tools.md](console-tools.md) — как пользоваться `kafka-console-producer.sh`/`kafka-console-consumer.sh`: отправка/чтение сообщений, ключи, consumer group, вывод метаданных.
- [partitions-and-replication.md](partitions-and-replication.md) — устройство партиций, репликация (Replication Factor, Leader/Follower, ISR), сегменты и retention/compaction, настройка `acks`, влияние числа партиций на параллелизм.
- [delivery-semantics.md](delivery-semantics.md) — гарантии доставки: at most once / at least once / exactly once, идемпотентный producer, транзакции, паттерн outbox.
- [kraft.md](kraft.md) — метаданные кластера и лидер-выборы: KRaft vs ZooKeeper, роль controller'а.
- [ecosystem.md](ecosystem.md) — Schema Registry, Kafka Connect, Kafka Streams/ksqlDB.
- [use-cases.md](use-cases.md) — паттерны, для которых обычно берут Kafka: CDC (Change Data Capture), Event Sourcing.

## Плюсы

- Очень высокий throughput (миллионы сообщений/сек), за счёт sequential I/O и zero-copy.
- Горизонтальное масштабирование через партиции и брокеры.
- Данные хранятся долго — можно replay истории, несколько независимых consumer-групп читают один топик по-своему.
- Отказоустойчивость через репликацию (ISR, leader election).
- Богатая экосистема: Connect, Streams, Schema Registry.
- Гарантия порядка внутри партиции + строгая упорядоченность по ключу.

## Минусы

- Сложность эксплуатации: тюнинг партиций, retention, ISR, ребалансировка consumer-групп — не тривиально.
- Нет встроенных сложных паттернов роутинга сообщений (в отличие от RabbitMQ с exchange'ами) — Kafka проще как "тупой" append-only лог.
- Ребалансировка (rebalancing) consumer-групп — перераспределение партиций между consumer'ами группы при входе/выходе участника или падении по heartbeat-таймауту — может временно останавливать обработку: старый (eager) протокол на время ребалансировки отбирает партиции у всех consumer'ов группы разом ("stop-the-world"), incremental rebalancing переназначает только те партиции, которые реально сменили владельца, не трогая остальные.
- Малые сообщения/большое количество топиков-партиций — оверхед на метаданные и файловую систему.
- Порог входа: нужно понимать партиционирование, offset-менеджмент, semantics доставки (at-least-once по умолчанию, exactly-once требует настройки).
- Дороже в поддержке, чем простые брокеры очередей, если не нужны все возможности (replay, high throughput).

## Когда использовать

- Event sourcing, CDC, стриминговая аналитика, агрегация логов, межсервисная шина событий с высокой нагрузкой — детали и примеры паттернов в [use-cases.md](use-cases.md).
- Не лучший выбор для простых задач "отправить одно сообщение — обработать — забыть" с низким объёмом — там проще RabbitMQ/SQS.
