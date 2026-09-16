# Семантика доставки (delivery semantics)

Как Kafka гарантирует (или не гарантирует), что сообщение будет обработано ровно один раз. Завязано на то, когда producer считает запись подтверждённой (см. `acks` в [partitions-and-replication.md](partitions-and-replication.md)) и когда consumer коммитит offset.

## Термины

- **At most once** — сообщение может быть потеряно, но никогда не обработается дважды. Consumer коммитит offset *до* обработки сообщения: если он упадёт после коммита, но до обработки — сообщение потеряно.
- **At least once** — сообщение никогда не потеряется, но может обработаться дважды. Consumer коммитит offset *после* обработки: если упадёт после обработки, но до коммита — при рестарте сообщение прочитается снова. Поведение по умолчанию в большинстве конфигураций Kafka.
- **Exactly once (EOS)** — сообщение обрабатывается ровно один раз. Достигается связкой: идемпотентный producer (`enable.idempotence=true`, убирает дубли при retry на уровне партиции) + транзакции (`transactional.id`, атомарная запись в несколько партиций/топиков) + consumer, читающий только `read_committed` сообщения. Работает надёжно только внутри Kafka (write → Kafka → read); если consumer после обработки пишет во внешнюю систему (БД, HTTP), exactly-once сквозь границу гарантировать сложно — нужен паттерн **outbox** (запись в БД и событие пишутся в одной локальной транзакции, событие в Kafka публикуется отдельным процессом из этой же таблицы-"outbox") или идемпотентный sink (внешняя система сама умеет отбрасывать дубли по ключу).

## Как это работает

| Семантика | Как достигается | Риск |
|---|---|---|
| At most once | commit offset до обработки | потеря сообщений |
| At least once | commit offset после обработки (default) | дубли, нужна идемпотентность consumer'а |
| Exactly once | idempotent producer + transactions + read_committed | сложнее в настройке, ниже throughput |

## Примеры использования

Consumer с ручным коммитом (at-least-once: коммитим offset после обработки):

```python
from confluent_kafka import Consumer

c = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "billing",
    "enable.auto.commit": False,  # коммитим сами
})
c.subscribe(["orders"])

while True:
    msg = c.poll(1.0)
    if msg is None:
        continue
    process(msg.value())   # сначала обработать
    c.commit(msg)          # потом закоммитить offset
```

Producer с идемпотентностью и транзакцией (exactly-once при записи в несколько партиций):

```python
from confluent_kafka import Producer

p = Producer({
    "bootstrap.servers": "localhost:9092",
    "enable.idempotence": True,
    "transactional.id": "billing-producer-1",
})
p.init_transactions()

p.begin_transaction()
p.produce("orders", key="user-42", value="order-created")
p.produce("audit-log", key="user-42", value="order-created-audit")
p.commit_transaction()  # обе записи атомарны: либо обе видны consumer'у, либо ни одной
```
