# Dynamic tables

NoSQL-слой YTsaurus: как хранить и обслуживать данные для точечных чтений/записей (в отличие от батчевой обработки static-таблиц из [computation.md](computation.md)), приближенно к OLTP/key-value нагрузке. Базовый термин Table — см. [README.md](README.md).

## Термины

- **Static table** — обычная таблица YTsaurus, используемая как вход/выход операций (см. [computation.md](computation.md)): неизменяемая после записи последовательность chunk'ов, оптимизирована под последовательное чтение большими блоками.
- **Dynamic table** — изменяемая таблица: поддерживает точечные insert/update/delete/lookup с низкой задержкой, физически разбита на tablets.
- **Tablet** — горизонтальный шард dynamic table: непрерывный диапазон ключей (для sorted dynamic table) или часть лога (для ordered dynamic table). Аналог партиции — см. [../../broker/kafka/partitions-and-replication.md](../../broker/kafka/partitions-and-replication.md) для сравнения с партицией Kafka.
- **Tablet cell** — группа tablet node'ов, реплицированная через Hydra (тот же механизм консенсуса, что и у master — см. [architecture.md](architecture.md)), обслуживающая один или несколько tablets с сохранением согласованности при отказе узла.
- **Tablet node** — узел, на котором физически исполняется tablet cell: держит "горячие" данные в памяти и периодически сбрасывает (flush) их в chunk'и на диске.
- **Sorted dynamic table** — dynamic table, упорядоченная по ключу; поддерживает lookup по ключу и range-запросы, аналогична распределённой отсортированной key-value таблице (как Bigtable/HBase).
- **Ordered dynamic table** — dynamic table без сортировки, запись только в конец (append-only log), строки адресуются по порядковому номеру — используется как очередь/лог, а не key-value хранилище. Построение pub/sub-очереди с несколькими независимыми читателями поверх такой таблицы — см. [queues.md](queues.md).
- **Pivot key** — граница диапазона ключей между соседними tablets sorted dynamic table. Список pivot keys, переданный команде `reshard-table`, определяет, сколько получится tablets и где проходят границы их диапазонов.

## Как это работает

1. При создании dynamic table задаётся [схема](architecture.md) и (для sorted) ключевые колонки (`sort_order=ascending`). Если не указать число tablets явно (атрибут `tablet_count`), таблица создаётся с одним tablet на весь диапазон ключей — разбить его на несколько позже можно командой `reshard-table`.
2. Каждый tablet назначается на tablet cell; несколько tablets могут обслуживаться одной cell, а одна большая таблица — множеством cells на разных tablet node.
3. Запись (insert/update) сначала попадает в память tablet node и в write-ahead log — журнал на диске для восстановления при падении узла до того, как данные сброшены в chunk.
4. Периодически данные из памяти сбрасываются (flush) в неизменяемый chunk на диске; фоновый процесс compaction объединяет chunk'и, чтобы чтение оставалось эффективным.
5. Lookup/range-запрос идёт на tablet node, отвечающий за нужный диапазон ключей — он совмещает данные из памяти и из chunk'ов на диске.

### Диапазоны ключей и распределение tablets (sorted dynamic table, 3 tablets)

| Tablet | Диапазон ключей | Tablet cell |
|---|---|---|
| T0 | `[-∞, "m")` | Cell 1 (Node 1) |
| T1 | `["m", "s")` | Cell 2 (Node 2) |
| T2 | `["s", +∞)` | Cell 3 (Node 3) |

Границы диапазонов выбираются так, чтобы объём данных и нагрузка на чтение/запись были распределены по tablets равномерно, а не поровну по алфавиту ключей. При перекосе нагрузки (один диапазон становится "горячим") границы можно пересчитать и перераспределить tablets между cells вручную или автоматически.

## Особенности/trade-offs

- Dynamic tables дают latency, сопоставимую с key-value БД, но это надстройка над той же chunk-инфраструктурой YTsaurus. Если нужна только OLTP-нагрузка без батчевой обработки, специализированная БД (Postgres, YDB) может быть проще в эксплуатации.
- Один и тот же путь в Cypress можно превратить из static в dynamic table (`mount-table`) и обратно — удобно для паттерна "загрузили батчем → включили serving-слой", но требует заранее спланированной схемы и ключей.

## Примеры использования

Создание sorted dynamic table со схемой и tablets, монтирование, `insert-rows`/`select-rows` и ручной `reshard-table` по pivot keys — команды собраны в разделе [«Dynamic tables» в cli.md](cli.md#dynamic-tables).
