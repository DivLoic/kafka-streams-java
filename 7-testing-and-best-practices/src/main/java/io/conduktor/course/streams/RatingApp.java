package io.conduktor.course.streams;

import static org.apache.kafka.streams.kstream.Named.as;

import io.conduktor.course.streams.avro.Product;
import io.conduktor.course.streams.avro.RatingAccumulator;
import io.conduktor.course.streams.avro.ScoreDetail;
import io.conduktor.course.streams.avro.TotalScore;
import io.conduktor.course.streams.operator.AccumulatorToTotalValueMapper;
import io.conduktor.course.streams.operator.RatingAccumulatorAggregator;
import io.conduktor.course.streams.operator.RatingProductValueJoiner;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RatingApp extends StreamingApp {

  // TODO: move rating to byte type

  private static final Logger logger = LoggerFactory.getLogger(RatingApp.class);

  public RatingApp() {
    this.config = ConfigFactory.load();
  }

  public RatingApp(Config config) {
    this.config = config;
  }

  public String productsTopic() {
    return config.getString("app.topics.input.products");
  }

  public String ratingsTopic() {
    return config.getString("app.topics.input.ratings");
  }

  public String scoresTopic() {
    return config.getString("app.topics.output.scores");
  }

  public String outputTopic() {
    return config.getString("app.topics.output.score-details");
  }

  public static Topology buildTopology(RatingApp app, TopologyParams params) {

    final StreamsBuilder builder = new StreamsBuilder();

    final KTable<String, Product> products = builder
        .table(app.productsTopic(), params.getProductConsumed("product-source"));

    final KStream<String, TotalScore> averages = builder
        .stream(app.ratingsTopic(), params.getRatingConsumed("rating-source"))

        .selectKey((zoneId, rating) -> rating.getProductId(), as("select-product-id-as-key"))

        .groupByKey(params.getRatingGrouped("rating-group"))

        .aggregate(
            () -> RatingAccumulator.newBuilder().build(),
            new RatingAccumulatorAggregator(),
            params.getRatingMaterialized("accumulator-materialization")

        )

        .mapValues(new AccumulatorToTotalValueMapper(), as("map-accumulator-to-total"))

        .toStream(as("total-scores-table-to-stream"))

        .repartition(params.getRatingRepartitioned(app.scoresTopic()));

    final KStream<String, ScoreDetail> ratingWithProduct = averages.join(
        products,
        new RatingProductValueJoiner(),
        params.getRatingProductJoined("product-join")
    );

    ratingWithProduct.to(app.outputTopic(), params.getRatingProduced("rating-output"));

    return builder.build();
  }

  public static void main(String[] args) {

    RatingApp app = new RatingApp();

    final TopologyParams params = new TopologyParams(
        app.configuredAvroSerde(),
        app.configuredAvroSerde(),
        app.configuredAvroSerde(),
        app.configuredAvroSerde(),
        app.configuredAvroSerde()
    );

    final Topology topology = buildTopology(app, params);

    KafkaStreams streams = new KafkaStreams(topology, app.getProperties());

    TopologyDescription description = topology.describe();
    logger.info(description.toString());

    streams.cleanUp(); // - Demo only
    streams.start();

    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }
}
