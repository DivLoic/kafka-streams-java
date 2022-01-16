package io.conducktor.course.streams;

import io.conducktor.course.streams.avro.OilType;
import io.conducktor.course.streams.avro.RawTemperature;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;

public abstract class TestUtils {

  protected final RawTemperature createTestTemperature(Integer temperature,
                                                       String fryer,
                                                       String store) {
    return RawTemperature
        .newBuilder()
        .setTemperature(temperature)
        .setFryer(fryer)
        .setStore(store)
        .setTimestamp(Instant.now())
        .build();
  }

  protected final RawTemperature createTestTemperature(Integer temperature,
                                                       String fryer,
                                                       String store,
                                                       Instant timestamp) {
    return RawTemperature
        .newBuilder(createTestTemperature(temperature, fryer, store))
        .setTimestamp(timestamp)
        .build();
  }

  protected final Headers createTestHeaders(OilType oilType) {
    return new RecordHeaders()
        .add(new RecordHeader("oil_type", oilType.toString().getBytes(StandardCharsets.UTF_8)))
        .add(new RecordHeader("os_version", "test_version".getBytes(StandardCharsets.UTF_8)))
        .add(new RecordHeader("precision", "test_precision".getBytes(StandardCharsets.UTF_8)));
  }
}
