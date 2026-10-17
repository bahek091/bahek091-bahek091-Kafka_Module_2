#!/bin/bash
set -e

echo 'Waiting for Kafka...'
until kafka-topics.sh --bootstrap-server kafka-0:9094 --list >/dev/null 2>&1; do
  sleep 2
done

kafka-topics.sh --bootstrap-server kafka-0:9094 --create --if-not-exists --topic messages --partitions 3 --replication-factor 1
kafka-topics.sh --bootstrap-server kafka-0:9094 --create --if-not-exists --topic filtered_messages --partitions 3 --replication-factor 1
kafka-topics.sh --bootstrap-server kafka-0:9094 --create --if-not-exists --topic blocked_users --partitions 1 --replication-factor 1 --config cleanup.policy=compact
kafka-topics.sh --bootstrap-server kafka-0:9094 --create --if-not-exists --topic banned_words --partitions 1 --replication-factor 1 --config cleanup.policy=compact

for t in messages filtered_messages blocked_users banned_words; do
  for i in $(seq 1 30); do
    kafka-topics.sh --bootstrap-server kafka-0:9094 --describe --topic "$t" 2>/dev/null \
      | grep -qE 'Leader: [0-9]+' && break
    [ "$i" -eq 30 ] && { echo "ERROR: $t has no leader after 60s"; exit 1; }
    sleep 2
  done
done

if [ -f /banned_words.txt ]; then
  echo 'Seeding banned_words...'
  while IFS= read -r word; do
    [ -z "$word" ] && continue
    printf "%s:true\n" "$word"
  done < /banned_words.txt | \
    kafka-console-producer.sh --bootstrap-server kafka-0:9094 \
      --topic banned_words --property "parse.key=true" --property "key.separator=:"
  sleep 2
fi

echo 'Topics ready.'