# Use cases: CDC и Event Sourcing

Два паттерна, ради которых чаще всего берут Kafka как основу архитектуры (см. также «Когда использовать» в [README.md](README.md)).

## Термины

- **CDC (Change Data Capture)** — паттерн отслеживания изменений в БД (insert/update/delete) и публикации их как потока событий. Kafka обычно выступает шиной для этих событий: коннектор (например Debezium через Kafka Connect, см. [ecosystem.md](ecosystem.md)) читает changelog/WAL базы и пишет каждое изменение строки в топик — другие сервисы подписываются и получают актуальное состояние без прямых запросов к БД.
- **Event Sourcing** — архитектурный паттерн, при котором состояние системы хранится не как текущий снимок (строка в таблице), а как последовательность событий, которые к этому состоянию привели (`OrderCreated`, `OrderPaid`, `OrderShipped`). Текущее состояние восстанавливается проигрыванием (replay) событий. Kafka подходит как хранилище такого лога событий, потому что: данные не удаляются после чтения, сообщения хранятся долго/бессрочно (retention или compaction — см. [partitions-and-replication.md](partitions-and-replication.md)), и порядок событий в партиции гарантирован.

## Примеры использования

CDC: сообщение, которое Debezium публикует в топик `pg.public.orders` при UPDATE строки в Postgres:

```json
{
  "before": {"id": 42, "status": "created"},
  "after":  {"id": 42, "status": "paid"},
  "op": "u",
  "source": {"table": "orders", "ts_ms": 1737020000000}
}
```

Event Sourcing: лог событий одного заказа в топике `orders-events` (partition key = `order_id`, порядок гарантирован):

```json
{"type": "OrderCreated", "order_id": 42, "user_id": 7,  "ts": "2026-01-01T10:00:00Z"}
{"type": "OrderPaid",    "order_id": 42, "amount": 990, "ts": "2026-01-01T10:02:00Z"}
{"type": "OrderShipped", "order_id": 42, "carrier": "dhl","ts": "2026-01-02T09:00:00Z"}
```

Текущее состояние заказа = fold (свёртка) этих трёх событий по порядку — так работает восстановление состояния в Event Sourcing.
