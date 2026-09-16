# yt CLI

Как установить, настроить и пользоваться консольным клиентом `yt` — единой точкой входа для команд из остальных аспектов (создание таблиц из [architecture.md](architecture.md), запуск операций из [computation.md](computation.md), работа с dynamic tables из [dynamic-tables.md](dynamic-tables.md) и очередями из [queues.md](queues.md)). Базовый термин Cluster — см. [README.md](README.md).

## Откуда взять

`yt` CLI не распространяется отдельным бинарником — ставится вместе с Python-клиентом:

```bash
pip install ytsaurus-client
```

Пакет `ytsaurus-client` даёт сразу и консольную команду `yt`, и Python-библиотеку `yt.wrapper` (используется в примерах из [ecosystem.md](ecosystem.md), [queues.md](queues.md)) — отдельно устанавливать библиотеку не нужно.

## Термины

- **Proxy** — адрес (хост или короткий алиас), через который клиент обращается к кластеру по HTTP/RPC; всё, что делает CLI, физически идёт через proxy к master'ам и node'ам кластера (см. [architecture.md](architecture.md)).
- **YT_PROXY** — переменная окружения (или ключ `proxy` в конфиге), задающая адрес кластера по умолчанию, чтобы не передавать его в каждой команде флагом.
- **YT_TOKEN** — переменная окружения (или ключ `token` в конфиге) с токеном аутентификации; выдаётся через веб-интерфейс кластера.
- **`~/.yt/config`** — YSON-файл с настройками CLI по умолчанию (proxy, token и другие параметры), читается автоматически, если переменные окружения не заданы.

## Как это работает

1. CLI резолвит адрес кластера: сначала смотрит на флаг `--proxy`, потом на `YT_PROXY`, потом на `~/.yt/config`.
2. Аналогично резолвится токен аутентификации (`--token` → `YT_TOKEN` → конфиг) и подставляется в заголовки запросов к proxy.
3. Каждая подкоманда `yt <command>` — это вызов конкретного метода HTTP/RPC API кластера (`get`, `create`, `map`, `insert-rows` и т.д.); CLI сериализует аргументы, отправляет запрос на proxy и печатает ответ в stdout.
4. Ответы по умолчанию печатаются в YSON; формат вывода/ввода можно переопределить флагом `--format` (`json`, `yson`, `dsv` и другие) — это использовалось в примерах остальных аспектов (`--format json`).

## Примеры использования

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

## Особенности/trade-offs

- Явный `--proxy`/`--token` на конкретной команде всегда перекрывает переменные окружения и конфиг — удобно для one-off команды на другой кластер без переключения настроек по умолчанию.
- Формат вывода по умолчанию (YSON) непривычен пользователям, ожидающим JSON — для скриптов и интеграции с другими инструментами обычно явно задают `--format json`.
