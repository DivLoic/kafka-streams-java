package io.conduktor.course.streams;

import static org.apache.kafka.streams.kstream.Named.as;

import io.conduktor.course.streams.avro.OilType;
import io.conduktor.course.streams.operator.MetaDataDecorator;
import io.conduktor.course.streams.operator.QualityPredictor;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.time.Duration;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Suppressed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeepFryerControlApp extends StreamingApp {

  static final Logger logger = LoggerFactory.getLogger(DeepFryerControlApp.class);

  public static final Duration WINDOW_SIZE = Duration.ofMinutes(1);
  public static final Duration WINDOW_GRACE = Duration.ZERO;
  public static final Long WINDOW_MAX_RECORDS = 999_999L;

  public DeepFryerControlApp() {
    this.config = ConfigFactory.load();
  }

  public DeepFryerControlApp(Config config) {
    this.config = config;
  }

  public String getRawTemperatureTopic() {
    return this.config.getString("app.topics.input.raw-temperatures");
  }

  public String getWarningTopic() {
    return this.config.getString("app.topics.output.warnings");
  }

  public static Topology buildTopology(DeepFryerControlApp app, TopologyParams params) {
    final StreamsBuilder builder = new StreamsBuilder();

    builder
        .stream(app.getRawTemperatureTopic(), params.getRawTemperatureConsumed())

        .transformValues(MetaDataDecorator::new, as("extract-headers"))

        .filter(($, temperature) -> temperature.getOilType().equals(OilType.VEGETABLE))

        .selectKey(($, temperature) -> temperature.getFryer())

        .groupByKey(params.getTemperatureGrouped("group-temp-by-session"))

        .windowedBy(SessionWindows.ofInactivityGapAndGrace(WINDOW_SIZE, WINDOW_GRACE))

        .count(as("count"), params.getMaxTemperatureMaterialized("max-temperature-sessions"))

        .suppress(
            Suppressed
                .untilWindowCloses(
                    Suppressed
                        .BufferConfig
                        .unbounded()
                        .withMaxRecords(WINDOW_MAX_RECORDS))
                .withName("suppress-max-temperature")
        );

    final Topology topology = builder.build();

    topology.addProcessor(
            "predict-quality",
            QualityPredictor::new,
            "suppress-max-temperature");

    topology.addSink(
        "produce-warning",
        app.getWarningTopic(),
        Serdes.String().serializer(),
        params.getQualityWarningSerializer(),
        "predict-quality");

    topology.connectProcessorAndStateStores(
        "predict-quality",
        "max-temperature-sessions");

    return topology;
  }

  public static void main(String[] args) {

    final DeepFryerControlApp app = new DeepFryerControlApp();

    final TopologyParams params = new TopologyParams(
        app.configuredAvroSerde(),
        app.configuredAvroSerde(),
        app.configuredAvroSerde()
    );

    final Topology topology = buildTopology(app, params);

    KafkaStreams streams = new KafkaStreams(topology, app.getProperties());

    TopologyDescription description = topology.describe();
    logger.info(description.toString());

    // TODO settled on the shutdown before or after start
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    // TODO: add cleanUp every where ?
    streams.cleanUp(); // - Demo only
    streams.start();

  }
}
