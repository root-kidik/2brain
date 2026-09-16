# YTsaurus

Распределённая платформа хранения и обработки больших данных экзабайтного масштаба — открытая версия внутренней системы Яндекса (YT), с 2023 года доступна как open source (ytsaurus.tech). В отличие от классического Hadoop-стека, где HDFS, YARN, HBase и Hive — отдельные системы, YTsaurus объединяет распределённое хранилище, MapReduce-подобные вычисления и NoSQL-хранилище (dynamic tables) в одной платформе с единым namespace и API.

## Ключевые термины

- **Cluster** — набор серверов (nodes) и master'ов, работающих как единая система хранения и вычислений. Клиент обращается к кластеру через proxy/CLI, а не к конкретному узлу напрямую.
- **Master** — реплицированная группа серверов, хранящая метаданные кластера: дерево [Cypress](architecture.md) и расположение [chunk'ов](architecture.md) на узлах. Подробнее о механизме репликации метаданных — в [architecture.md](architecture.md).
- **Cypress** — виртуальное дерево объектов кластера (таблицы, файлы, директории), через которое адресуются все данные, например `//home/project/orders`. Подробнее — в [architecture.md](architecture.md).
- **Chunk** — единица физического хранения данных; таблица — это упорядоченная последовательность chunk'ов, реплицированных по узлам. Подробнее — в [architecture.md](architecture.md).
- **Node** — физический узел кластера, выполняющий одну или несколько ролей: хранение chunk'ов, выполнение вычислительных jobs или обслуживание dynamic tables. Роли разбираются в [architecture.md](architecture.md), [computation.md](computation.md) и [dynamic-tables.md](dynamic-tables.md) соответственно.
- **Table** — основная единица данных в YTsaurus: либо неизменяемая static table (батчевые данные), либо изменяемая dynamic table (точечные чтения/записи). Разница и механика — в [dynamic-tables.md](dynamic-tables.md).
- **Operation** — распределённая вычислительная задача над таблицами (Map, Reduce, Sort и т.д.), которую кластер разбивает на параллельные jobs. Подробнее — в [computation.md](computation.md).
- **Scheduler** — компонент, распределяющий jobs операций по узлам кластера с учётом ресурсов и квот. Подробнее — в [computation.md](computation.md).

## Разделы

- [cli.md](cli.md) — как пользоваться `yt` CLI: установка, настройка подключения к кластеру (proxy/token), базовые команды навигации по Cypress.
- [architecture.md](architecture.md) — устройство хранения: master и Cypress, chunk'и и их репликация/erasure coding, роли data node/exec node.
- [computation.md](computation.md) — batch-вычисления: операции Map/Reduce/MapReduce/Sort, scheduler и fair-share pools, data locality.
- [dynamic-tables.md](dynamic-tables.md) — NoSQL-слой: tablets, tablet cells, sorted/ordered dynamic tables как OLTP/serving хранилище.
- [queues.md](queues.md) — pub/sub-очереди поверх ordered dynamic table: consumer'ы, offset (row index), trim/retention, идемпотентная запись.
- [ecosystem.md](ecosystem.md) — доступ к данным поверх низкоуровневого API: YQL, CHYT (ClickHouse over YTsaurus), SPYT (Spark over YTsaurus), CLI/SDK.

## Плюсы

- Единая платформа: батчевое хранилище + вычисления + OLTP-слой (dynamic tables) + SQL-движки в одном кластере, без склейки HDFS+YARN+HBase+Hive.
- Экзабайтный масштаб, обкатан в продакшене Яндекса больше 10 лет до открытия исходников.
- Erasure coding для холодных данных снижает overhead хранения по сравнению с полной репликацией при сравнимой надёжности (см. [architecture.md](architecture.md)).
- Data locality: scheduler размещает jobs на узлах, где физически лежат нужные chunk'и, снижая сетевой трафик.
- Иерархические pools с fair-share scheduling дают предсказуемое разделение ресурсов кластера между командами.
- Ad-hoc SQL (CHYT, YQL) читает те же таблицы напрямую, без отдельного ETL в аналитическую БД.

## Минусы

- Открыт с 2023 года — комьюнити, туториалы и сторонние интеграции заметно меньше, чем у Hadoop/Spark/Trino.
- Много типов компонентов (master, scheduler, data/exec/tablet node) — эксплуатационная сложность сопоставима с Hadoop-стеком, только внутри одной системы вместо нескольких.
- Master — точка входа за метаданными; на экстремальных масштабах Cypress шардируют по нескольким master-ячейкам, и это отдельная настройка, а не поведение по умолчанию.
- Собственные API/CLI (`yt`, YQL) — не drop-in замена HDFS/Hadoop API, перенос существующих pipeline'ов требует переписывания.
- Экспертиза и документация исторически ориентированы на паттерны использования внутри Яндекса, а не на широкий внешний опыт эксплуатации.

## Когда использовать

- Нужен один кластер сразу под батчевую обработку больших объёмов данных, под serving-слой с точечными чтениями/записями ([dynamic tables](dynamic-tables.md)) и под ad-hoc SQL-аналитику ([CHYT](ecosystem.md)) — без склеивания нескольких отдельных систем.
- Не лучший выбор, если уже есть работающий cloud-native стек (S3 + Spark/Trino + отдельная OLTP БД): там больше готовых интеграций и шире комьюнити, а переезд на YTsaurus добавит операционных затрат без явного выигрыша.
