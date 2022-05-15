package io.conduktor.course.streams;

import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FavouriteMealApp {

  private static Logger logger = LoggerFactory.getLogger(FavouriteMealApp.class);

  public static void main(String[] args) {
    Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "favourite-meal-app");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    // for demo purpose: brings immediate feedback (default: 3000)
    config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);

    StreamsBuilder builder = new StreamsBuilder();

    KStream<String, String> textLines = builder.stream("user-profile-updates");

    KTable<String, String> favouriteMealsCount = textLines

        .filter((key, value) -> value.contains(",goto-meal:"))

        .selectKey((key, value) -> value.split(",")[0].toLowerCase())

        .mapValues(value -> value.split(":")[1].toLowerCase())

        .groupBy((user, meal) -> meal)

        .count()

        .mapValues((count) -> Long.toString(count));

    favouriteMealsCount.toStream().to("favourite-meals-output");

    KafkaStreams streams = new KafkaStreams(builder.build(), config);
    // only do this in dev - not in prod
    streams.cleanUp();
    streams.start();

    // print the topology
    logger.info(streams.toString());

    // shutdown hook to correctly close the streams application
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }
}
