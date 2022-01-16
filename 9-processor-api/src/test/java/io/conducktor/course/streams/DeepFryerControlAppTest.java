package io.conducktor.course.streams;

import io.conducktor.course.streams.avro.OilType;
import io.conducktor.course.streams.avro.RawTemperature;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.time.Duration;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DeepFryerControlAppTest extends TestUtils {

  private TopologyTestDriver testDriver;
  private TestInputTopic<Bytes, RawTemperature> temperatureInputTopic;

  private final Config config = ConfigFactory.parseResources("unit-tests/main.conf");

  private final DeepFryerControlApp app = new DeepFryerControlApp(config);
  private final Topology topology = DeepFryerControlApp.buildTopology(app, new TopologyParams(
      app.configuredAvroSerde(),
      app.configuredAvroSerde(),
      app.configuredAvroSerde()
  ));

  @Before
  public void setTopologyTestDriver() {
    //testDriver = new TopologyTestDriver(topology, app.getProperties());

    temperatureInputTopic = testDriver.createInputTopic(
        app.getRawTemperatureTopic(),
        Serdes.Bytes().serializer(),
        app.<RawTemperature>configuredAvroSerde().serializer()
    );
  }

  @After
  public void tearDown() {
    testDriver.close();
  }

  public void sendQualityWarningTest() {
    // Given
    final RawTemperature temperature = createTestTemperature(50, "F1", "S1");

    final TestRecord<Bytes, RawTemperature> testRecord =
        new TestRecord<>(null, temperature, createTestHeaders(OilType.VEGETABLE));

    // When
    temperatureInputTopic.pipeInput(testRecord);

    testDriver.advanceWallClockTime(Duration.ofSeconds(5));

  }

}