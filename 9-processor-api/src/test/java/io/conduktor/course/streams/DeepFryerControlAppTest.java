package io.conduktor.course.streams;

import static java.time.temporal.ChronoUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.conduktor.course.streams.avro.OilType;
import io.conduktor.course.streams.avro.QualityWarning;
import io.conduktor.course.streams.avro.RawTemperature;
import java.time.Duration;
import java.time.Instant;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.UnlimitedWindow;
import org.apache.kafka.streams.state.internals.InMemoryTimeOrderedKeyValueBuffer;
import org.apache.kafka.streams.test.TestRecord;
import org.assertj.core.api.Condition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DeepFryerControlAppTest extends TestUtils {

  private TopologyTestDriver testDriver;
  private TestInputTopic<Bytes, RawTemperature> temperatureInputTopic;
  private TestOutputTopic<String, QualityWarning> tpmOutputTopic;

  private final Config config = ConfigFactory.parseResources("unit-tests/main.conf");

  private final DeepFryerControlApp app = new DeepFryerControlApp(config);
  private final Topology topology = DeepFryerControlApp.buildTopology(app, new TopologyParams(
      app.configuredAvroSerde(),
      app.configuredAvroSerde(),
      app.configuredAvroSerde()
  ));

  @Before
  public void setTopologyTestDriver() {
    testDriver = new TopologyTestDriver(topology, app.getProperties());

    temperatureInputTopic = testDriver.createInputTopic(
        app.getRawTemperatureTopic(),
        Serdes.Bytes().serializer(),
        app.<RawTemperature>configuredAvroSerde().serializer()
    );

    tpmOutputTopic = testDriver.createOutputTopic(
        app.getWarningTopic(),
        Serdes.String().deserializer(),
        app.<QualityWarning>configuredAvroSerde().deserializer()
    );
  }

  @After
  public void tearDown() {
    testDriver.close();
  }

  @Test
  public void sendQualityWarningTest() {
    // Given
    final Instant time = Instant.now().minus(Duration.ofMinutes(25));

    final TestRecord<Bytes, RawTemperature> testRecord = new TestRecord<>(null,
        createTestTemperature(50, "F902", "S1"),
        createTestHeaders(OilType.VEGETABLE),
        time);

    // When
    temperatureInputTopic.pipeInput(testRecord);

    testDriver.advanceWallClockTime(Duration.ofSeconds(10));

    final QualityWarning warning =
        QualityWarning.newBuilder().setTpmPrediction("15.0").setSession(time).build();
    // Then
    assertThat(tpmOutputTopic.readKeyValuesToList())
        .containsExactly(new KeyValue<>("F902", warning));
  }

  @Test
  public void sendQualityWarningForANewSession() {
    // Given
    final Instant previousSession = Instant.now().minus(Duration.ofHours(1));
    final Instant currentSession = Instant.now().minus(Duration.ofMinutes(25));

    final TestRecord<Bytes, RawTemperature> testRecord =
        new TestRecord<>(null,
            createTestTemperature(50, "F912", "S1"),
            createTestHeaders(OilType.VEGETABLE),
            currentSession);

    final TestRecord<Bytes, RawTemperature> testRecord2 =
        new TestRecord<>(null,
            createTestTemperature(50, "F912", "S1"),
            createTestHeaders(OilType.VEGETABLE),
            previousSession);

    // When
    temperatureInputTopic.pipeInput(testRecord);
    temperatureInputTopic.pipeInput(testRecord2);

    testDriver.advanceWallClockTime(Duration.ofSeconds(10));

    // Then
    assertThat(tpmOutputTopic.getQueueSize()).isEqualTo(1);

    assertThat(tpmOutputTopic.readRecord().value())
        .satisfies(
            new Condition<>(warning -> warning.getSession()
                                           .truncatedTo(SECONDS)
                                           .compareTo(currentSession.truncatedTo(SECONDS)) == 0,
                "Warns about the correct session"));

  }

  @Test
  public void sendQualityWarningForASameSession() {
    // Given
    final Instant measureOne = Instant.now().minus(Duration.ofMinutes(24));
    final Instant measureTwo = Instant.now().minus(Duration.ofMinutes(22));
    final Instant measureThree = Instant.now().minus(Duration.ofMinutes(20));

    final TestRecord<Bytes, RawTemperature> testRecord =
        new TestRecord<>(null,
            createTestTemperature(50, "F922", "S1"),
            createTestHeaders(OilType.VEGETABLE),
            measureOne);

    final TestRecord<Bytes, RawTemperature> testRecord2 =
        new TestRecord<>(null,
            createTestTemperature(50, "F922", "S1"),
            createTestHeaders(OilType.VEGETABLE),
            measureTwo);

    final TestRecord<Bytes, RawTemperature> testRecord3 =
        new TestRecord<>(null,
            createTestTemperature(50, "F922", "S1"),
            createTestHeaders(OilType.VEGETABLE),
            measureThree);

    // When
    temperatureInputTopic.pipeRecordList(
        Stream.of(testRecord, testRecord2, testRecord3)
            .collect(Collectors.toList()));

    testDriver.advanceWallClockTime(Duration.ofSeconds(10));

    // Then
    assertThat(tpmOutputTopic.getQueueSize()).isEqualTo(1);

    assertThat(tpmOutputTopic.readRecord().value())
        .satisfies(
            new Condition<>(warning -> warning.getSession()
                                           .truncatedTo(SECONDS)
                                           .compareTo(measureOne.truncatedTo(SECONDS)) == 0,
                "Warns about the correct session"));
  }

  @Test
  public void suppressPreviousSessionTest() {
    // Given
    final Instant previousSession = Instant.now().minus(Duration.ofHours(1));
    final Instant currentSession = Instant.now()
        .minus(Duration.ofHours(1))
        .plus(Duration.ofMinutes(16));

    final TestRecord<Bytes, RawTemperature> testRecord =
        new TestRecord<>(null,
            createTestTemperature(50, "F932", "S1"),
            createTestHeaders(OilType.VEGETABLE),
            previousSession);

    final TestRecord<Bytes, RawTemperature> testRecord2 =
        new TestRecord<>(null,
            createTestTemperature(50, "F933", "S1"),
            createTestHeaders(OilType.VEGETABLE),
            currentSession);

    // When
    temperatureInputTopic.pipeInput(testRecord);
    //temperatureInputTopic.pipeInput(testRecord2);

    testDriver.advanceWallClockTime(Duration.ofSeconds(10));

    final InMemoryTimeOrderedKeyValueBuffer<Windowed<String>, QualityWarning> stateStore =
        (InMemoryTimeOrderedKeyValueBuffer<Windowed<String>, QualityWarning>) testDriver.getStateStore(
            "suppress-max-temperature-store");

    // Then
    assertThat(stateStore.priorValueForBuffered(new Windowed("F932", new UnlimitedWindow(previousSession.toEpochMilli()))))
        .satisfies(new Condition<>(iterator -> !iterator.isDefined(), ""));
  }
}