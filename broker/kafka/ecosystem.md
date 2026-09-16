# Экосистема

Инструменты вокруг Kafka для интеграции с внешними системами, управления схемами сообщений и потоковой обработки — обычно от Confluent или сообщества, не входят в core Kafka, но идут с ней рука об руку.

## Термины

- **Schema Registry** — отдельный сервис (обычно от Confluent), хранит схемы сообщений (Avro/Protobuf/JSON Schema) с версионированием. Producer перед отправкой сериализует сообщение по схеме и кладёт в сообщение только ID схемы (не саму схему), consumer по ID достаёт схему из registry и десериализует. Обеспечивает совместимость версий (schema evolution) — consumer со старой схемой не падает на новом поле.
- **Kafka Connect** — фреймворк для интеграции Kafka с внешними системами без написания кода. **Source-коннектор** читает данные из внешней системы (БД, файл, API) и пишет в топик. **Sink-коннектор** читает из топика и пишет во внешнюю систему (Elasticsearch, S3, БД). Работает как отдельный процесс/кластер (Connect workers), конфигурируется JSON-конфигом.
- **Kafka Streams** — Java-библиотека для потоковой обработки данных прямо поверх Kafka (без отдельного кластера обработки типа Spark/Flink): читает из одного топика, трансформирует/агрегирует, пишет в другой.
- **ksqlDB** — SQL-надстройка над Kafka Streams: потоковые трансформации и агрегации пишутся как SQL-запросы вместо Java-кода.

## Как это работает

- Schema Registry: producer перед отправкой проверяет/регистрирует схему сообщения в registry (по HTTP), получает её ID и кладёт этот ID в заголовок сообщения; consumer читает ID из сообщения, запрашивает у registry саму схему (с локальным кэшированием) и десериализует payload.
- Kafka Connect: коннектор (source или sink) конфигурируется JSON-конфигом и разбивается на несколько parallel **tasks**, которые Connect worker'ы распределяют между собой — похоже на партиции у обычного producer/consumer, только единицей параллелизма выступает task, а не партиция топика.
- Kafka Streams / ksqlDB: приложение читает входной топик как непрерывный поток, строит топологию обработки (map/filter/join/window-агрегация) и пишет результат в выходной топик; состояние агрегаций (например, `COUNT(*)` в примере ниже) хранится локально в RocksDB на инстансе приложения и бэкапится в служебный топик с [log compaction](partitions-and-replication.md).

## Примеры использования

Конфиг source-коннектора Kafka Connect (Debezium, CDC из PostgreSQL — подробнее про сам паттерн CDC см. [use-cases.md](use-cases.md)):

```json
{
  "name": "orders-postgres-source",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "localhost",
    "database.dbname": "shop",
    "table.include.list": "public.orders",
    "topic.prefix": "pg"
  }
}
```

Регистрация Avro-схемы в Schema Registry:

```bash
curl -X POST http://localhost:8081/subjects/orders-value/versions \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"schema": "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}"}'
```

Простая агрегация в ksqlDB (сколько заказов на пользователя за последний час):

```sql
CREATE TABLE orders_per_user AS
  SELECT user_id, COUNT(*) AS cnt
  FROM orders
  WINDOW TUMBLING (SIZE 1 HOUR)
  GROUP BY user_id;
```
