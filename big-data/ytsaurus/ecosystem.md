# Экосистема

Как получить доступ к данным YTsaurus помимо низкоуровневого API: SQL-движки поверх кластера и клиентские инструменты. Базовые термины Table/Cluster — см. [README.md](README.md).

## Термины

- **YQL** — SQL-подобный язык запросов, изначально созданный в Яндексе для YTsaurus (тогда — внутренней системы YT), сейчас используется и для других хранилищ (например YDB). Запрос выполняется через отдельный компонент — query tracker, который транслирует YQL в набор операций (Map/Reduce/Sort — см. [computation.md](computation.md)) над static-таблицами.
- **CHYT** ("ClickHouse over YTsaurus") — интеграция ClickHouse с YTsaurus: движок ClickHouse запускается как долгоживущая операция (clique) на exec node'ах кластера и читает/пишет таблицы YTsaurus напрямую, давая быстрый ad-hoc SQL без выгрузки данных наружу.
- **Clique** — набор процессов ClickHouse, запущенных как одна CHYT-операция; клиент подключается к clique как к обычному ClickHouse-серверу по протоколу ClickHouse.
- **SPYT** ("Spark over YTsaurus") — интеграция Apache Spark: Spark executors запускаются как jobs YTsaurus-операции, а Spark читает/пишет таблицы YTsaurus через отдельный коннектор вместо HDFS/S3.
- **`yt` CLI / SDK** — консольная утилита и клиентские библиотеки (Python, Java, Go, C++) для работы с Cypress, static/dynamic таблицами и запуска операций программно, без ClickHouse/Spark/YQL.

## Как это работает

- YQL-запрос отправляется в query tracker → он строит план из стандартных YT-операций (Map/Reduce/Sort/Merge) → план выполняется scheduler'ом так же, как операция, запущенная напрямую через `yt map-reduce`.
- CHYT clique — долгоживущий процесс (в отличие от обычной операции, которая завершается после обработки данных): один раз поднятый clique обслуживает множество последующих SQL-запросов без накладных расходов на повторный запуск jobs.
- SPYT поднимает Spark driver и executors как jobs YTsaurus-операции, после чего с ними работают как с обычным Spark-кластером — тот же код на PySpark/Scala, что и для Hadoop/S3, только источник данных — таблицы YTsaurus.

## Примеры использования

```bash
# SQL-запрос к таблице через YQL
yql -s <cluster> -e "SELECT user_id, count(*) FROM \`//home/project/orders\` GROUP BY user_id"

# поднять CHYT clique и выполнить запрос через clickhouse-client
yt clickhouse start-clique --instance-count 5 --alias ch_project
clickhouse-client --host <proxy> --port 8123 --query \
  "SELECT count(*) FROM \"//home/project/orders\""
```

```python
# Python SDK: чтение таблицы без CLI
import yt.wrapper as yt

for row in yt.read_table("//home/project/orders"):
    print(row)
```
