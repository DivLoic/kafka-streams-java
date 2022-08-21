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
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "fruit-count-app");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    // for demo purpose: set this to scale on the same machine
    // config.put(StreamsConfig.STATE_DIR_CONFIG, System.getenv("statedirectory"));

    // for demo purpose: brings immediate feedback (default: 3000)
    config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0);

    StreamsBuilder builder = new StreamsBuilder();
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
    KafkaStreams streams = new KafkaStreams(topology, config);

    TopologyDescription description = topology.describe();
    logger.info(description.toString());

    streams.cleanUp();
    streams.start();

    // shutdown hook to correctly close the streams application
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }
}
