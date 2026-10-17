package yandex.praktikum;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;

/**
 * Топология:
 *   messages ──> CensorProcessor (цензура + фильтр блокировок) ──> filtered_messages
 */
public class CensorStreamsApp {

    public static final String MESSAGES_TOPIC = "messages";
    public static final String FILTERED_TOPIC = "filtered_messages";
    public static final String BLOCKED_USERS_TOPIC = "blocked_users";
    public static final String BANNED_WORDS_TOPIC = "banned_words";

    public static final String BLOCKED_STORE = "blocked-users-store";
    public static final String BANNED_STORE = "banned-words-store";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "censor-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"));
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");

        StreamsBuilder builder = new StreamsBuilder();

        GlobalKTable<String, String> bannedWords = builder.globalTable(
                BANNED_WORDS_TOPIC,
                Materialized.<String, String>as(Stores.persistentKeyValueStore(BANNED_STORE))
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String())
        );

        GlobalKTable<String, String> blockedUsers = builder.globalTable(
                BLOCKED_USERS_TOPIC,
                Materialized.<String, String>as(Stores.persistentKeyValueStore(BLOCKED_STORE))
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.String())
        );

        builder.stream(MESSAGES_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
                .process(CensorProcessor::new, Named.as("censor-processor"))
                .to(FILTERED_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close, "streams-shutdown-hook"));
    }
}