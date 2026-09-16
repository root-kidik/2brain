# Очереди (Queues)

Как YTsaurus реализует pub/sub-очередь поверх ordered dynamic table: несколько независимых читателей обрабатывают один и тот же поток данных на своей скорости и с сохранением позиции при перезапуске. Базовый термин Ordered dynamic table — см. [dynamic-tables.md](dynamic-tables.md).

## Термины

- **Queue** — ordered dynamic table (см. [dynamic-tables.md](dynamic-tables.md)), используемая через специальное queue API: строки только дописываются в конец, каждый tablet таблицы работает как независимый partition очереди.
- **Partition** — здесь то же самое, что tablet ordered dynamic table в контексте очереди: строки внутри partition строго упорядочены и нумеруются возрастающим row index.
- **Row index** — порядковый номер строки внутри partition очереди, аналог offset в Kafka (см. [../../broker/kafka/README.md](../../broker/kafka/README.md#ключевые-термины)).
- **Producer** — клиент, дописывающий строки в конец partition очереди обычной записью в ordered dynamic table. Для идемпотентной записи (без дублей при retry) используется отдельный объект **Queue producer**: клиент присваивает записям возрастающий sequence number, и при повторной отправке после сетевого сбоя дубликаты по этому номеру отбрасываются — аналог idempotent producer в Kafka.
- **Consumer** — объект YTsaurus (тоже таблица специального формата), хранящий последний прочитанный row index отдельно по каждой partition очереди. Несколько consumer'ов могут независимо читать одну и ту же очередь с разной скоростью и с разных позиций — аналогично Kafka consumer group (см. [../../broker/kafka/README.md](../../broker/kafka/README.md#ключевые-термины)), но здесь позиция хранится в явном объекте, а не автоматически привязывается к `group.id`.
- **Registration** — явная связь consumer'а с конкретной queue (`register_queue_consumer`), которую нужно создать до того, как этот consumer сможет читать очередь и продвигать по ней свою позицию.
- **Queue agent** — сервис кластера, отслеживающий зарегистрированные очереди и consumer'ов: считает lag (отставание consumer'а от конца очереди по каждой partition) и выполняет auto trim.
- **Trim / auto trim** — удаление старых строк из начала partition, чтобы очередь не росла бесконечно; настраивается по retention (TTL) вручную или автоматически через queue agent — аналог retention в Kafka (см. [../../broker/kafka/partitions-and-replication.md](../../broker/kafka/partitions-and-replication.md)).

## Как это работает

1. Producer пишет строки в конец нужных partitions (tablets) очереди — как в обычную ordered dynamic table.
2. Consumer регистрируется на очередь (`register_queue_consumer`) — после этого он может независимо от других consumer'ов запрашивать порции строк, начиная со своего сохранённого row index по каждой partition. Модель pull: consumer сам запрашивает следующую порцию, очередь ничего не проталкивает.
3. После обработки порции consumer продвигает (`advance_consumer`) свою сохранённую позицию по прочитанным partitions. Если consumer упал до advance — при перезапуске он получит те же строки заново (at-least-once).
4. Queue agent в фоне сравнивает row index consumer'а с последним row index в partition (lag) и, если включён auto trim, удаляет из начала partition строки старше настроенного retention.

### Несколько независимых consumer'ов на одной очереди (2 partitions)

| Partition | Последний row index (голова) | Consumer "billing" (прочитано до) | Consumer "analytics" (прочитано до) |
|---|---|---|---|
| P0 | 1042 | 1040 (lag=2) | 900 (lag=142) |
| P1 | 980 | 980 (lag=0) | 850 (lag=130) |

Оба consumer'а читают одни и те же две partitions независимо: у каждого своя пара (partition → row index), поэтому "billing" почти не отстаёт от головы очереди, а "analytics" обрабатывает данные с большей задержкой — и это не мешает "billing".

```
Producer ──► Queue (ordered dynamic table)
                 ├── Partition P0 ──► Consumer "billing"    (свой row index на P0)
                 │                └─► Consumer "analytics"  (свой row index на P0)
                 └── Partition P1 ──► Consumer "billing"    (свой row index на P1)
                                  └─► Consumer "analytics"  (свой row index на P1)
```

## Особенности/trade-offs

- В Kafka offset consumer group появляется автоматически при первом чтении под этим `group.id`. В YTsaurus consumer — явный объект в Cypress, который нужно создать и зарегистрировать за очередью заранее — чуть больше ручной настройки, но зато consumer управляется как обычный объект (права доступа через ACL Cypress, отдельный мониторинг lag через queue agent).
- Очередь — это тот же dynamic table, поэтому параллелизм чтения ограничен заранее заданным числом tablets (partitions), как и у обычных dynamic tables (см. [dynamic-tables.md](dynamic-tables.md)) — увеличить его на лету так же нетривиально, как реshardинг обычной dynamic table.

## Примеры использования

```bash
# создать ordered dynamic table и использовать её как очередь
yt create table //home/project/events_queue --attributes '{dynamic=%true; schema=[{name=payload;type=string}]}'
yt mount-table //home/project/events_queue

# создать объект consumer и зарегистрировать его на очередь
yt create queue_consumer //home/project/events_queue_consumer_billing
yt mount-table //home/project/events_queue_consumer_billing
yt register-queue-consumer //home/project/events_queue //home/project/events_queue_consumer_billing --vital

# записать строки в очередь (обычная запись в ordered dynamic table)
yt insert-rows //home/project/events_queue '[{"payload": "order-created"}]' --format json
```

```python
# Python SDK: consumer читает свою позицию и продвигает её после обработки
import yt.wrapper as yt

rows = yt.pull_consumer(
    consumer_path="//home/project/events_queue_consumer_billing",
    queue_path="//home/project/events_queue",
    partition_index=0,
    offset=1040,
    max_row_count=100,
)
for row in rows:
    process(row)

yt.advance_consumer(
    consumer_path="//home/project/events_queue_consumer_billing",
    queue_path="//home/project/events_queue",
    partition_index=0,
    old_offset=1040,
    new_offset=1040 + len(rows),
)
```
