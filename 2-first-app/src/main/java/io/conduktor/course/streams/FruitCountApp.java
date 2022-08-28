package io.conduktor.course.streams;

import java.util.Properties;
import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.StreamsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FruitCountApp {

  public static void main(String[] args) {
    Logger logger = LoggerFactory.getLogger(FruitCountApp.class);

    Properties config = new Properties();
    // TODO (2): Set APPLICATION_ID_CONFIG to "fruit-count-app"
    // TODO (3): Set BOOTSTRAP_SERVERS_CONFIG to "localhost:9092"
    // TODO (4): Set AUTO_OFFSET_RESET_CONFIG to "earliest"
    // TODO (5): Set DEFAULT_KEY_SERDE_CLASS_CONFIG to String
    // TODO (6): Set DEFAULT_VALUE_SERDE_CLASS_CONFIG to String

    // for demo purpose: set this to scale on the same machine
    // config.put(StreamsConfig.STATE_DIR_CONFIG, System.getenv("statedirectory"));

    // for demo purpose: brings immediate feedback (default: 3000)
    // TODO (7): Set COMMIT_INTERVAL_MS_CONFIG to 0

    // TODO (8): instantiate a StreamsBuilder
    StreamsBuilder builder = null;
    // 1 - stream from Kafka

    KStream<String, String> fruitEvents = builder.stream("fruit-input");
    KTable<String, Long> fruitCounts = fruitEvents
        // 2 - map values to lowercase
        .mapValues((ValueMapper<String, String>) String::toLowerCase)
        // can be alternatively written as:
        // .mapValues(String::toLowerCase)
        // 3 - flatmap values split by space
        .flatMapValues(textLine -> Arrays.asList(textLine.split("\\W+")))
        // 4 - select key to apply a key (we discard the old key)
        .selectKey((key, word) -> word)
        // 5 - group by key before aggregation
        .groupByKey()
        // 6 - count occurrences
        .count(Materialized.as("Counts"));

    // 7 - map the long value to string
    KTable<String, String> toStrFruitCounts =
        fruitCounts.mapValues((count) -> Long.toString(count));

    // 8 - to in order to write the results back to kafka
    toStrFruitCounts.toStream().to("fruit-output-count");

    Topology topology = builder.build();
    // TODO (9): instantiate a StreamsBuilder
    KafkaStreams streams = null;

    TopologyDescription description = topology.describe();
    logger.info(description.toString());

    streams.cleanUp(); // dev only
    streams.start();

    // shutdown hook to correctly close the streams application
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }
}
