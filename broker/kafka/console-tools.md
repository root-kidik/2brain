# Консольные CLI-инструменты

Как пользоваться штатными консольными утилитами Kafka для ручной отправки и чтения сообщений — без написания кода, для отладки и проверки топиков. Базовые термины Topic/Partition/Offset/Consumer Group — см. [README.md](README.md).

## Откуда взять

Скрипты не распространяются отдельно — это часть официальной поставки Apache Kafka:

- **Официальный tarball** — kafka.apache.org → Downloads, распаковать архив: скрипты лежат в `bin/` (на Windows — их аналоги `.bat` в `bin/windows/`).
- **Docker-образ** — без локальной установки Kafka, например `confluentinc/cp-kafka` или `bitnami/kafka`: зайти в контейнер (`docker exec -it <container> bash`) и вызвать те же команды.
- **Пакетные менеджеры** — например `brew install kafka` на macOS кладёт бинарники в `PATH` (там они без суффикса `.sh`, но делают то же самое).

## Термины

- **kafka-console-producer.sh** — консольная утилита-producer: читает строки со stdin (или из файла) и отправляет каждую строку как отдельное сообщение в указанный топик.
- **kafka-console-consumer.sh** — консольная утилита-consumer: подписывается на топик и печатает прочитанные сообщения в stdout.
- **`--property`** — общий флаг обеих утилит для настройки формата ввода/вывода сообщений (например, разбор ключа у producer'а или печать метаданных у consumer'а), без изменения кода.
- **`--producer.config`/`--consumer.config`** — флаг producer- и consumer-утилиты соответственно, указывающий на properties-файл с настройками подключения, которые не умещаются в отдельные флаги — в первую очередь аутентификация (SASL, SSL) для защищённых кластеров.

## Как это работает

- `kafka-console-producer.sh` построчно читает stdin: каждая строка — отдельное сообщение, отправляемое в указанный топик (в партицию — по ключу, если он задан, иначе round-robin, как обычный producer — см. [README.md](README.md)).
- `kafka-console-consumer.sh` подключается к брокерам через `--bootstrap-server`, читает сообщения из партиций топика и печатает их в stdout; поведение (с какого места читать, сохранять ли позицию) определяется флагами `--from-beginning` и `--group`.
- Обе утилиты — это тонкие обёртки над обычным producer/consumer API: то, что видно в консоли, — то же, что получил бы код на Java/Python, использующий тот же топик.

## Примеры использования

### kafka-console-producer.sh

```bash
# отправить сообщения вручную (ввод с клавиатуры построчно, Ctrl+D — завершить)
kafka-console-producer.sh --topic orders --bootstrap-server localhost:9092

# отправить сообщение с явным ключом: формат ввода "key:value", разделитель задаётся отдельно
kafka-console-producer.sh --topic orders --bootstrap-server localhost:9092 \
  --property parse.key=true --property key.separator=:
# > user-42:order-created

# отправить содержимое файла: каждая строка файла — отдельное сообщение
kafka-console-producer.sh --topic orders --bootstrap-server localhost:9092 < events.txt
```

### kafka-console-consumer.sh

```bash
# прочитать топик с начала (--from-beginning игнорирует сохранённый offset consumer'а)
kafka-console-consumer.sh --topic orders --from-beginning --bootstrap-server localhost:9092

# читать как часть именованной consumer group — offset сохраняется на брокере,
# при перезапуске с тем же --group чтение продолжится с места остановки, а не с начала
kafka-console-consumer.sh --topic orders --group billing --bootstrap-server localhost:9092

# показать ключ, партицию и offset каждого сообщения, а не только значение
kafka-console-consumer.sh --topic orders --bootstrap-server localhost:9092 \
  --property print.key=true --property print.partition=true --property print.offset=true

# прочитать только N сообщений и остановиться (удобно для отладки)
kafka-console-consumer.sh --topic orders --from-beginning --max-messages 5 --bootstrap-server localhost:9092
```

Без `--group` каждый запуск `kafka-console-consumer.sh` создаёт новую consumer group со случайным именем: offset нигде осмысленно не сохраняется, и повторный запуск без `--from-beginning` начнёт читать только новые сообщения (с `latest`), а не продолжит с прошлого места.

### Подключение к кластеру с аутентификацией

`--bootstrap-server` в примерах выше подходит для локального кластера без аутентификации. Для защищённого кластера (SASL/SSL) настройки подключения выносятся в отдельный properties-файл и передаются флагом `--consumer.config` (у producer-утилиты — `--producer.config`):

```properties
# client.properties
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="user" password="pass";
```

```bash
kafka-console-consumer.sh --topic orders --bootstrap-server broker1.example.com:9093 \
  --consumer.config client.properties
```

## Особенности/trade-offs

- Обе утилиты — инструменты для отладки и ручной проверки, а не для продакшен-нагрузки: нет батчинга, ретраев с настраиваемой политикой, обработки ошибок сериализации — это то, что даёт настоящий клиент (Java/Python producer/consumer API).
- Сообщения передаются и печатаются как обычный текст (по строкам); бинарные форматы (Avro/Protobuf из [ecosystem.md](ecosystem.md)) консольными утилитами напрямую не десериализуются — нужны отдельные консольные обёртки (например `kafka-avro-console-consumer` при Schema Registry).
