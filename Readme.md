# Kafka Streams: блокировка пользователей и цензура сообщений

## Классы и логика

**`Message`** — POJO с полями `recipientId` и `text`. Ключ в Kafka — `senderId`.

**`CensorStreamsApp`** — точка входа. Создаёт две `GlobalKTable` (`banned_words`, `blocked_users`), подключает поток `messages` к `CensorProcessor`, пишет результат в `filtered_messages`.

**`CensorProcessor`** — для каждого сообщения: маскирует запрещённые слова, затем проверяет блокировку по ключу `recipientId:senderId`. Если значение `block` — сообщение отбрасывается.

**`CensorUtils`** — маскировка слов через regex `\p{L}+` (поддержка кириллицы). Найденные слова заменяются на `***` с сохранением пунктуации.

**State store** — оба списка хранятся в RocksDB (`/tmp/kafka-streams`) и восстанавливаются из компактных топиков. Обновляются динамически: запись в `banned_words`/`blocked_users` подхватывается без перезапуска.

**Топики:**
- `messages` — входящие сообщения;
- `filtered_messages` — результат обработки;
- `blocked_users` — ключ `ownerId:blockedId`, значение `block`/`unblock`;
- `banned_words` — ключ — слово, значение `true`.

## Запуск

```bash
docker compose down -v
docker compose up -d --build
docker compose logs -f kafka-setup # ждать "Topics ready."
docker compose logs -f streams-app # ждать "State transition to RUNNING"
```



Kafka UI: http://localhost:8080

Остановка: 
`docker compose down`, с удалением данных: `docker compose down -v`.

## Тестирование

Заблокировать `bob` пользователем `alice`:


## Инструкция по тестированию
Все команды выполняются из терминала хоста. Отправка и чтение — через контейнер kafka-0 (внутренний адрес kafka-0:9094).

Шаг 1. Заблокировать пользователя bob пользователем alice:

```bash
echo "alice:bob|block" | \
  docker exec -i $(docker ps -qf "name=kafka-0") \
  kafka-console-producer.sh --bootstrap-server kafka-0:9094 \
  --topic blocked_users --property "parse.key=true" --property "key.separator=|"
```
Шаг 2. Отправить тестовые сообщения в messages (см. следующий раздел).

Шаг 3. Прочитать результат из `filtered_messages`:

```bash
docker exec -it $(docker ps -qf "name=kafka-0") \
  kafka-console-consumer.sh --bootstrap-server kafka-0:9094 \
  --topic filtered_messages --from-beginning \
  --property print.key=true --property key.separator=" -> "
```
Ожидаемый результат:

```bash
сообщение alice -> carol с badword — слово замаскировано на ***;

сообщение bob -> alice — отсутствует (bob заблокирован);

сообщение bob -> carol — прошло без изменений.
```
Проверка динамического обновления слов. Добавить новое слово и отправить сообщение с ним — оно начнёт маскироваться без перезапуска приложения:

```bash
printf "newword:true\n" | \
  docker exec -i $(docker ps -qf "name=kafka-0") \
  kafka-console-producer.sh --bootstrap-server kafka-0:9094 \
  --topic banned_words --property "parse.key=true" --property "key.separator=:"
```
Проверка персистентности. Перезапустить приложение и убедиться, что списки сохранились:

```bash
docker compose restart streams-app
```

## Тестовые данные
### `banned_words`

Загружаются автоматически из `banned_words.txt` при старте `kafka-setup`:
```text
badword
запрещено
spam
```
В топик blocked_users (блокировки)
```text
alice:bob|block
```
Ключ — alice:bob, значение — block. 
Снятие блокировки: `alice:bob|unblock`.

### `messages`
```bash
# 1) цензура: badword должно стать ***
echo 'alice:{"recipientId":"carol","text":"Hello carol, this is a badword message"}' | \
  docker exec -i $(docker ps -qf "name=kafka-0") \
  kafka-console-producer.sh --bootstrap-server kafka-0:9094 \
  --topic messages --property "parse.key=true" --property "key.separator=:"

# 2) блокировка: bob заблокирован alice → сообщение не должно дойти
echo 'bob:{"recipientId":"alice","text":"Hi alice"}' | \
  docker exec -i $(docker ps -qf "name=kafka-0") \
  kafka-console-producer.sh --bootstrap-server kafka-0:9094 \
  --topic messages --property "parse.key=true" --property "key.separator=:"

# 3) bob → carol: не заблокирован → проходит без изменений
echo 'bob:{"recipientId":"carol","text":"Hello carol from bob"}' | \
  docker exec -i $(docker ps -qf "name=kafka-0") \
  kafka-console-producer.sh --bootstrap-server kafka-0:9094 \
  --topic messages --property "parse.key=true" --property "key.separator=:"
```
Ожидаемый вывод в filtered_messages:

```text
alice -> {"recipientId":"carol","text":"Hello carol, this is a *** message"}
bob -> {"recipientId":"carol","text":"Hello carol from bob"}
```