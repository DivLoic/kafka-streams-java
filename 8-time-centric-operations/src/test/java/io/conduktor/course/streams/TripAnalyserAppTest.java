package io.conduktor.course.streams;

import static org.assertj.core.api.Assertions.assertThat;

import io.conduktor.course.streams.avro.Arrival;
import io.conduktor.course.streams.avro.Departure;
import io.conduktor.course.streams.avro.VehicleType;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.test.TestRecord;
import org.assertj.core.api.Condition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TripAnalyserAppTest extends TestUtils {

  private TopologyTestDriver testDriver;
  private TestInputTopic<String, Departure> departureInputTopic;
  private TestInputTopic<String, Arrival> arrivalInputTopic;
  private TestOutputTopic<Windowed<String>, Long> shortTripCountOutputTopic;

  private final Config config = ConfigFactory.parseResources("unit-tests/main.conf");

  private final TripAnalyserApp app = new TripAnalyserApp(config);
  private final Topology topology = TripAnalyserApp.buildTopology(app, new TopologyParams(
      app.configuredAvroSerde(),
      app.configuredAvroSerde(),
      app.configuredAvroSerde()
  ));

  @Before
  public void setTopologyTestDriver() {
    testDriver = new TopologyTestDriver(topology, app.getProperties());

    departureInputTopic = testDriver.createInputTopic(
        app.departuresTopic(),
        Serdes.String().serializer(),
        app.<Departure>configuredAvroSerde().serializer()
    );

    arrivalInputTopic = testDriver.createInputTopic(
        app.arrivalsTopic(),
        Serdes.String().serializer(),
        app.<Arrival>configuredAvroSerde().serializer()
    );

    shortTripCountOutputTopic = testDriver.createOutputTopic(
        app.shortTripCountTopic(),
        WindowedSerdes.timeWindowedSerdeFrom(String.class, Duration.ofDays(1).toMillis())
            .deserializer(),
        Serdes.Long().deserializer()
    );
  }

  @After
  public void tearDown() {
    testDriver.close();
  }


  @Test
  public void joinDepartureToArrivalTest() {
    // Given
    final String tripId1 = "TRIP_ID1";
    final Instant departureTime = Instant.parse("2022-01-05T09:00:00Z");
    final Instant arrivalTime = Instant.parse("2022-01-05T09:11:00Z");

    final Departure departure =
        createTestDeparture("UT_CAR", "UT_STORE", departureTime);
    final Arrival arrival =
        createTestArrival("UT_CAR", "UT_STORE", arrivalTime);

    // When
    final TestRecord<String, Departure> departureTestRecord =
        new TestRecord<>(tripId1, departure, Instant.now().minus(Duration.ofHours(1)));

    final TestRecord<String, Arrival> arrivalTestRecord =
        new TestRecord<>(tripId1, arrival, Instant.now());

    departureInputTopic.pipeInput(departureTestRecord);
    arrivalInputTopic.pipeInput(arrivalTestRecord);

    // Then
    final TimeWindow resultWindow = new TimeWindow(
        beginningOfTheDay(departureTime).toEpochMilli(),
        beginningOfNextDay(departureTime).toEpochMilli()
    );

    assertThat(shortTripCountOutputTopic.readKeyValuesToMap())
        .containsExactlyEntriesOf(Map.of(new Windowed<>("UT_STORE", resultWindow), 1L));

    final KeyValueIterator<Windowed<String>, Long> testIterator =
        testDriver.<String, Long>getWindowStore("daily-short-trip-count")
            .backwardFetchAll(beginningOfTheDay(departureTime), beginningOfNextDay(departureTime));

    assertThat(testIterator.next())
        .satisfies(new Condition<>((kv) -> kv.key.key().equals("UT_STORE"), "Store name"))
        .satisfies(new Condition<>((kv) -> kv.value.equals(1L), "Trip count"));

    assertThat(testIterator)
        .satisfies(new Condition<>((it) -> !it.hasNext(), "Trip saved in state"));

    testIterator.close();
  }
}