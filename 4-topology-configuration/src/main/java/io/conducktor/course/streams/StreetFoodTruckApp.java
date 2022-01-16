package io.conducktor.course.streams;

import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreetFoodTruckApp {

  private static Logger logger = LoggerFactory.getLogger(StreetFoodTruckApp.class);

  public static void main(String[] args) {

    Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "street-food-truck-app");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

    // we do not set DEFAULT_<KEY/VALUE>_SERDE_CLASS_CONFIG
    // default is: null

    StreamsBuilder builder = new StreamsBuilder();

    // defines the deserialization process
    final Consumed<Integer, Short> consumedAsOrder =
        Consumed.with(Serdes.Integer(), Serdes.Short());

    // defines the serialisation process
    final Produced<Long, String> producedAsNotification =
        Produced.with(Serdes.Long(), Serdes.String());

    KStream<Integer, Short> orders =
        builder.stream("street-food-orders", consumedAsOrder);

    KStream<Long, String> processedOrders = orders
        // changes the type of the key
        .selectKey(
            (Integer orderNumber, Short itemCount) -> Long.valueOf(orderNumber),
            Named.as("change-the-type-of-the-key")
        )

        // changes the type of the value
        .mapValues((Long orderNumber, Short itemCount) -> String.format(
                "Processing the order: ORDER-%s, preparing %s items", orderNumber, itemCount),
            Named.as("change-the-type-of-the-value")
        )

        // do something
        .peek((Long key, String value) -> logger.warn(value),
            Named.as("do-something")
        );

    // creates an error branch street-food-order-error
    final Branched<Long, String> branchedToError =
        Branched.withConsumer(
            stream -> stream.to("street-food-order-errors", producedAsNotification));

    // creates a default branch street-food-notifications
    final Branched<Long, String> branchedToDefault =
        Branched.withConsumer(
            stream -> stream.to("street-food-notifications", producedAsNotification));

    processedOrders.split()
        .branch((key, value) -> key == -1, branchedToError)
        .defaultBranch(branchedToDefault);

    runTheApp(builder, config);
  }

  private static void runTheApp(StreamsBuilder builder, Properties config) {
    Topology topology = builder.build();
    KafkaStreams streams = new KafkaStreams(topology, config);

    TopologyDescription description = topology.describe();
    logger.info(description.toString());

    streams.start();

    // shutdown hook to correctly close the streaming application
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }
}
