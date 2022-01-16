package io.conducktor.course.streams;

import static org.apache.kafka.streams.kstream.Named.as;

import io.conducktor.course.streams.avro.Arrival;
import io.conducktor.course.streams.avro.Departure;
import io.conducktor.course.streams.operator.DepartureArrivalStreamJoiner;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.time.Duration;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TripAnalyserApp extends StreamingApp {

  private static final Logger logger = LoggerFactory.getLogger(TripAnalyserApp.class);

  static final Duration JOIN_SLIDING_WINDOW_LENGTH = Duration.ofMinutes(15);
  static final Duration JOIN_SLIDING_WINDOW_GRACE = Duration.ofHours(1);
  static final Duration TUMBLING_WINDOW_GRACE = JOIN_SLIDING_WINDOW_GRACE;
  static final Duration GROUP_TUMBLING_WINDOW_LENGTH = Duration.ofDays(1);
  static final Duration GROUP_TUMBLING_WINDOW_ADVANCE_BY = GROUP_TUMBLING_WINDOW_LENGTH;

  public TripAnalyserApp() {
    this.config = ConfigFactory.load();
  }

  public TripAnalyserApp(Config config) {
    this.config = config;
  }

  public String departuresTopic() {
    return config.getString("app.topics.input.departures");
  }

  public String arrivalsTopic() {
    return config.getString("app.topics.input.arrivals");
  }

  public String shortTripCountTopic() {
    return config.getString("app.topics.output.short-trip-counts");
  }

  public static Topology buildTopology(TripAnalyserApp app, TopologyParams params) {
    final StreamsBuilder builder = new StreamsBuilder();

    final KStream<String, Departure> departures =
        builder.stream(app.departuresTopic(), params.getDepartureConsumed());

    final KStream<String, Arrival> arrivals =
        builder.stream(app.arrivalsTopic(), params.getArrivalConsumed());

    departures

        .join(arrivals,
            new DepartureArrivalStreamJoiner(),
            JoinWindows
                .of(JOIN_SLIDING_WINDOW_LENGTH)
                .grace(JOIN_SLIDING_WINDOW_GRACE),
            params.getDepartureArrivalStreamJoined("departure-cross-arrival"))

        .selectKey((orderId, trip) -> trip.getStoreId())

        .groupByKey(params.getShortTripCountGrouped())

        .windowedBy(TimeWindows
            .ofSizeAndGrace(
                GROUP_TUMBLING_WINDOW_LENGTH,
                TUMBLING_WINDOW_GRACE)
            .advanceBy(GROUP_TUMBLING_WINDOW_ADVANCE_BY))

        .count(as("short-trip-counter"),
            TopologyParams.getShortTripCountMaterialized("daily-short-trip-count"))

        .toStream()

        .to(app.shortTripCountTopic(),
            TopologyParams.getWindowedCountProduced(GROUP_TUMBLING_WINDOW_LENGTH));

    return builder.build();
  }

  public static void main(String[] args) {

    final TripAnalyserApp app = new TripAnalyserApp();

    final TopologyParams params = new TopologyParams(
        app.configuredAvroSerde(),
        app.configuredAvroSerde(),
        app.configuredAvroSerde()
    );

    final Topology topology = buildTopology(app, params);

    KafkaStreams streams = new KafkaStreams(topology, app.getProperties());

    TopologyDescription description = topology.describe();
    logger.info(description.toString());

    // TODO: add cleanUp every where ?
    streams.cleanUp(); // - Demo only
    streams.start();

    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }
}
