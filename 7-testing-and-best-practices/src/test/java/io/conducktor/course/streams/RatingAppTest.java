package io.conducktor.course.streams;

import static org.assertj.core.api.Assertions.assertThat;

import io.conducktor.course.streams.avro.Product;
import io.conducktor.course.streams.avro.Rating;
import io.conducktor.course.streams.avro.ScoreDetail;
import io.conducktor.course.streams.avro.TotalScore;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RatingAppTest {
  // TODO create the mapper test

  private TopologyTestDriver testDriver;
  private TestInputTopic<String, Product> productInputTopic;
  private TestInputTopic<String, Rating> ratingInputTopic;
  private TestOutputTopic<String, TotalScore> totalOutputTopic;
  private TestOutputTopic<String, ScoreDetail> detailedOutputTopic;

  private final Config config = ConfigFactory.parseResources("unit-tests/main.conf");

  private final RatingApp app = new RatingApp(config);
  private final Topology topology = RatingApp.buildTopology(app, new TopologyParams(
      app.configuredAvroSerde(),
      app.configuredAvroSerde(),
      app.configuredAvroSerde(),
      app.configuredAvroSerde(),
      app.configuredAvroSerde()
  ));

  @Before
  public void setTopologyTestDriver() {
    System.out.println(topology.describe().toString());
    testDriver = new TopologyTestDriver(topology, app.getProperties());

    productInputTopic = testDriver.createInputTopic(
        app.productsTopic(),
        Serdes.String().serializer(),
        app.<Product>configuredAvroSerde().serializer()
    );
    ratingInputTopic = testDriver.createInputTopic(
        app.ratingsTopic(),
        Serdes.String().serializer(),
        app.<Rating>configuredAvroSerde().serializer());

    totalOutputTopic = testDriver.createOutputTopic(
        // Prefix with app.id and suffix with "repartition"
        "unittest-" + app.scoresTopic() + "-repartition",
        Serdes.String().deserializer(),
        app.<TotalScore>configuredAvroSerde().deserializer()
    );

    detailedOutputTopic = testDriver.createOutputTopic(
        app.outputTopic(),
        Serdes.String().deserializer(),
        app.<ScoreDetail>configuredAvroSerde().deserializer()
    );
  }

  @After
  public void tearDown() {
    testDriver.close();
  }

  @Test
  public void groupRatingByProductIdTest() {
    ratingInputTopic.pipeRecordList(
        List.of(
            new TestRecord<>("",
                Rating.newBuilder().setProductId("P1").setUserId("U1").setScore(2).build()),
            new TestRecord<>("",
                Rating.newBuilder().setProductId("P2").setUserId("U2").setScore(5).build()),
            new TestRecord<>("",
                Rating.newBuilder().setProductId("P1").setUserId("U3").setScore(4).build())
        )
    );

    assertThat(totalOutputTopic.readKeyValuesToList()).containsExactly(
        KeyValue.pair("P1", TotalScore.newBuilder().setScore(2).setRatings(1).build()),
        KeyValue.pair("P2", TotalScore.newBuilder().setScore(5).setRatings(1).build()),
        KeyValue.pair("P1", TotalScore.newBuilder().setScore(3).setRatings(2).build()));
  }

  @Test
  public void joinScoreToProductOnIdTest() {
    productInputTopic.pipeRecordList(
        List.of(
        new TestRecord<>("P3",
            Product.newBuilder().setProductId("P3").setName("Mushroom Risotto").build()),
        new TestRecord<>("P4",
            Product.newBuilder().setProductId("p4").setName("Shrimp Risotto").build())
        )
    );

    ratingInputTopic.pipeRecordList(
        List.of(
            new TestRecord<>("",
                Rating.newBuilder().setProductId("P3").setUserId("U21").setScore(1).build()),
            new TestRecord<>("",
                Rating.newBuilder().setProductId("P3").setUserId("U22").setScore(4).build()),
            new TestRecord<>("",
                Rating.newBuilder().setProductId("P3").setUserId("U23").setScore(3).build()),
            new TestRecord<>("",
                Rating.newBuilder().setProductId("P4").setUserId("U24").setScore(5).build()),
            new TestRecord<>("",
                Rating.newBuilder().setProductId("P5").setUserId("U23").setScore(4).build())
        )
    );

    assertThat(detailedOutputTopic.readKeyValuesToMap()).containsExactly(
        Map.entry("P3", ScoreDetail.newBuilder().setScore(2.67).setName("Mushroom Risotto").build()),
        Map.entry("P4", ScoreDetail.newBuilder().setScore(5).setName("Shrimp Risotto").build())
    );
  }
}
