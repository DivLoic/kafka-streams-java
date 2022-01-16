package io.conducktor.course.streams;

import io.conducktor.course.streams.avro.Product;
import io.conducktor.course.streams.avro.ScoreDetail;
import io.conducktor.course.streams.avro.TotalScore;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RatingEnricher {

  private final static Logger logger = LoggerFactory.getLogger(RatingEnricher.class);

  public static void main(String[] args) {

    Properties config = new Properties();

    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "rating-enricher-app");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    config.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "local-instance1");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0);
    config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

    Map<String, Object> avroSerdeConfig = Collections.singletonMap(
        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "http://localhost:8081"
    );

    final SpecificAvroSerde<Product> productSerde = new SpecificAvroSerde<>();
    final SpecificAvroSerde<TotalScore> scoreSerde = new SpecificAvroSerde<>();
    final SpecificAvroSerde<ScoreDetail> scoreDetailSerde = new SpecificAvroSerde<>();

    productSerde.configure(avroSerdeConfig, false);
    scoreSerde.configure(avroSerdeConfig, false);
    scoreDetailSerde.configure(avroSerdeConfig, false);

    final StreamsBuilder builder = new StreamsBuilder();

    final GlobalKTable<String, Product> products =
        builder.globalTable("products",
            Consumed
                .with(Serdes.String(), productSerde)
                .withName("product-source")
        );

    final KStream<String, TotalScore> ratings =
        builder.stream("total-scores",
            Consumed
                .with(Serdes.String(), scoreSerde)
                .withName("score-source")
        );

    final KStream<String, ScoreDetail> ratedProducts = ratings
        .join(
            products,
            (key, value) -> key,
            (rating, product) -> ScoreDetail
                .newBuilder()
                .setName(product.getName())
                .setScore(rating.getScore())
                .build(),
            Named.as("product-join")
        );

    ratedProducts
        .to("score-details",
            Produced
                .with(Serdes.String(), scoreDetailSerde)
                .withName("produce-rated-product")
        );

    Topology topology = builder.build();
    KafkaStreams streams = new KafkaStreams(topology, config);

    TopologyDescription description = topology.describe();
    logger.info(description.toString());

    streams.start();

    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }
}
