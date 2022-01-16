package io.conducktor.course.streams;

import io.conducktor.course.streams.avro.Rating;
import io.conducktor.course.streams.avro.RatingAccumulator;
import io.conducktor.course.streams.avro.TotalScore;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Collections;
import java.util.List;
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
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RatingAggregator {

  private static final Logger logger = LoggerFactory.getLogger(RatingAggregator.class);

  public static void main(String[] args) {

    Properties config = new Properties();

    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "rating-aggregator-app");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    config.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "local-instance1");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 0);
    config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

    final SpecificAvroSerde<Rating> ratingSerde = new SpecificAvroSerde<>();
    final SpecificAvroSerde<RatingAccumulator> ratingAccumulatorSerde = new SpecificAvroSerde<>();
    final SpecificAvroSerde<TotalScore> scoreSerde = new SpecificAvroSerde<>();

    Map<String, Object> avroSerdeConfig = Collections.singletonMap(
        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
        "http://localhost:8081"
    );

    ratingSerde.configure(avroSerdeConfig, false);
    ratingAccumulatorSerde.configure(avroSerdeConfig, false);
    scoreSerde.configure(avroSerdeConfig, false);

    final StreamsBuilder builder = new StreamsBuilder();

    builder
        .stream(
            "ratings",
            Consumed
                .with(Serdes.Long(), ratingSerde)
                .withName("rating-source")
        )

        .selectKey(
            (zoneId, rating) -> rating.getProductId(),
            Named.as("select-product-id-as-key")
        )

        .groupByKey(
            Grouped
                .with(Serdes.String(), ratingSerde)
                .withName("rating-group")
        )

        .aggregate(
            () -> RatingAccumulator.newBuilder().build(),
            (productId, rating, aggregate) -> {

              final List<Integer> scores = aggregate.getScores();
              scores.add(rating.getScore());

              return RatingAccumulator
                  .newBuilder()
                  .setScores(scores)
                  .build();
            },
            Named.as("accumulator-materialization"),
            Materialized.with(Serdes.String(), ratingAccumulatorSerde)
        )

        .mapValues(accumulator -> {
          Integer numberOfRating = accumulator.getScores().size();
          Integer sumOfAllRatings = accumulator.getScores().stream().reduce(0, Integer::sum);

          final BigDecimal score = BigDecimal.valueOf(sumOfAllRatings)
              .divide(
                  BigDecimal.valueOf(numberOfRating),
                  2,
                  RoundingMode.UP
              );

          return TotalScore
              .newBuilder()
              .setRatings(numberOfRating)
              .setScore(score.doubleValue())
              .build();
        })

        .toStream()

        .to(
            "total-scores",
            Produced
                .with(Serdes.String(), scoreSerde)
                .withName("score-output")
        );

    Topology topology = builder.build();
    KafkaStreams streams = new KafkaStreams(topology, config);

    TopologyDescription description = topology.describe();
    logger.info(description.toString());

    streams.start();

    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }
}
