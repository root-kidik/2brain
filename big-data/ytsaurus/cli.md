# yt CLI

Как установить, настроить и пользоваться консольным клиентом `yt`. Все конкретные команды `yt`, которые встречаются в этой папке, собраны здесь в одном месте; остальные аспекты ([architecture.md](architecture.md), [computation.md](computation.md), [dynamic-tables.md](dynamic-tables.md), [queues.md](queues.md), [ecosystem.md](ecosystem.md)) объясняют, что делает та или иная команда и зачем, и ссылаются сюда за синтаксисом, а не дублируют его. Базовый термин Cluster — см. [README.md](README.md).

## Откуда взять

`yt` CLI не распространяется отдельным бинарником — ставится вместе с Python-клиентом:

```bash
pip install ytsaurus-client
```

Пакет `ytsaurus-client` даёт сразу и консольную команду `yt`, и Python-библиотеку `yt.wrapper` (используется в примерах Python SDK из [ecosystem.md](ecosystem.md) и [queues.md](queues.md)) — отдельно устанавливать библиотеку не нужно.

## Термины

- **Proxy** — адрес (хост или короткий алиас), через который клиент обращается к кластеру по HTTP/RPC; всё, что делает CLI, физически идёт через proxy к master'ам и node'ам кластера (см. [architecture.md](architecture.md)).
- **YT_PROXY** — переменная окружения (или ключ `proxy` в конфиге), задающая адрес кластера по умолчанию, чтобы не передавать его в каждой команде флагом.
- **YT_TOKEN** — переменная окружения (или ключ `token` в конфиге) с токеном аутентификации; выдаётся через веб-интерфейс кластера.
- **`~/.yt/config`** — YSON-файл с настройками CLI по умолчанию (proxy, token и другие параметры), читается автоматически, если переменные окружения не заданы.
- **Attributes** — набор именованных свойств объекта Cypress (например `replication_factor`, `schema`, `dynamic`, `tablet_count` у таблицы). Задаётся флагом `--attributes` в формате YSON-map (`'{key=value; key2=value2}'`) при создании объекта или читается/меняется отдельно (`yt get path/@key`, `yt set path/@key value`).
- **YSON** — формат сериализации данных YTsaurus (аналог JSON, но с более компактным синтаксисом: `{key=value; key2=value2}` вместо `{"key": "value", "key2": "value2"}`, поддерживает бинарное представление). Используется по умолчанию для конфигов CLI, значений `--attributes` и вывода команд; при необходимости заменяется флагом `--format` на `json`, `dsv` и другие.

## Как это работает

1. CLI резолвит адрес кластера: сначала смотрит на флаг `--proxy`, потом на `YT_PROXY`, потом на `~/.yt/config`.
2. Аналогично резолвится токен аутентификации (`--token` → `YT_TOKEN` → конфиг) и подставляется в заголовки запросов к proxy.
3. Каждая подкоманда `yt <command>` — это вызов конкретного метода HTTP/RPC API кластера (`get`, `create`, `map`, `insert-rows` и т.д.); CLI сериализует аргументы, отправляет запрос на proxy и печатает ответ в stdout.
4. Ответы по умолчанию печатаются в YSON; формат вывода/ввода можно переопределить флагом `--format` (`json`, `yson`, `dsv` и другие) — в примерах ниже почти везде используется `--format json`.

## Примеры использования

### Настройка и базовая навигация

```bash
# настроить кластер по умолчанию на текущей сессии
export YT_PROXY=my-cluster.example.com
export YT_TOKEN=$(cat ~/.yt/token)

# или сохранить настройки в конфиг, чтобы не экспортировать переменные каждый раз
yt config set proxy my-cluster.example.com
yt config set token "$(cat ~/.yt/token)"

# проверить, под каким пользователем подключились
yt whoami

# базовая навигация по Cypress — как по обычной файловой системе
yt list //home
yt exists //home/project/orders
yt get //home/project/orders/@type

# скопировать/переместить/удалить объект в Cypress
yt copy //home/project/orders //home/project/orders_backup
yt move //home/project/orders_backup //archive/orders_backup
yt remove //archive/orders_backup --recursive
```

### Static tables

Создание, запись и просмотр атрибутов хранения обычной таблицы — механика chunk'ов, replication factor и схемы разобрана в [architecture.md](architecture.md).

```bash
# создать таблицу со схемой (список колонок: имя + тип) и явным replication factor
yt create table //home/project/orders --attributes '{
  replication_factor=3;
  schema=[
    {name=order_id;type=int64};
    {name=user_id;type=int64};
    {name=amount;type=double}
  ]
}'

# создать таблицу без схемы — можно писать строки с произвольным набором колонок
yt create table //home/project/logs

# записать данные построчно (JSON) в таблицу
echo '{"order_id": 1, "user_id": 42, "amount": 100.0}
{"order_id": 2, "user_id": 7, "amount": 200.0}' | yt write-table //home/project/orders --format json

# посмотреть, из скольких chunk'ов состоит таблица и её атрибуты хранения
yt get //home/project/orders/@chunk_count
yt get //home/project/orders/@replication_factor
yt get //home/project/orders/@schema
```

### Операции (Map/Sort/MapReduce)

Что такое jobs, scheduler и pools за этими командами — см. [computation.md](computation.md).

```bash
# Map: применить скрипт к каждой строке независимо
yt map "python3 process.py" \
  --src //home/project/orders \
  --dst //home/project/orders_processed

# Sort по ключу перед Reduce
yt sort --src //home/project/orders_processed \
  --dst //home/project/orders_sorted \
  --sort-by "user_id"

# MapReduce: map + группировка по ключу + reduce за один запуск
yt map-reduce \
  --mapper "python3 mapper.py" \
  --reducer "python3 reducer.py" \
  --map-output-table //tmp/intermediate \
  --reduce-by "user_id" \
  --src //home/project/orders \
  --dst //home/project/orders_by_user
```

### Dynamic tables

Что такое tablets, tablet cells и pivot keys за этими командами — см. [dynamic-tables.md](dynamic-tables.md).

```bash
# создать sorted dynamic table со схемой, ключом (user_id) и сразу 3 tablets
yt create table //home/project/users_dynamic --attributes \
  '{dynamic=%true;
    schema=[{name=user_id;type=int64;sort_order=ascending};{name=name;type=string}];
    tablet_count=3}'

# смонтировать таблицу, чтобы tablet cells начали её обслуживать — без этого insert/select не работают
yt mount-table //home/project/users_dynamic

# точечная запись и чтение по ключу
yt insert-rows //home/project/users_dynamic '[{"user_id": 42, "name": "alice"}]' --format json
yt select-rows "* from [//home/project/users_dynamic] where user_id = 42"

# перераспределить диапазоны ключей вручную на уже существующей таблице (нужно предварительно unmount-table)
yt unmount-table //home/project/users_dynamic
yt reshard-table //home/project/users_dynamic --pivot-keys '[[]; [1000]; [2000]]'
yt mount-table //home/project/users_dynamic
```

### Очереди

Что такое consumer, partition и trim за этими командами — см. [queues.md](queues.md) (там же — чтение очереди из кода через Python SDK).

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

### CHYT и YQL

Что такое clique и query tracker за этими командами — см. [ecosystem.md](ecosystem.md).

```bash
# SQL-запрос к таблице через YQL
yql -s <cluster> -e "SELECT user_id, count(*) FROM \`//home/project/orders\` GROUP BY user_id"

# поднять CHYT clique и выполнить запрос через clickhouse-client
yt clickhouse start-clique --instance-count 5 --alias ch_project
clickhouse-client --host <proxy> --port 8123 --query \
  "SELECT count(*) FROM \"//home/project/orders\""
```

## Особенности/trade-offs

- Явный `--proxy`/`--token` на конкретной команде всегда перекрывает переменные окружения и конфиг — удобно для one-off команды на другой кластер без переключения настроек по умолчанию.
- Формат вывода по умолчанию (YSON) непривычен пользователям, ожидающим JSON — для скриптов и интеграции с другими инструментами обычно явно задают `--format json`.
