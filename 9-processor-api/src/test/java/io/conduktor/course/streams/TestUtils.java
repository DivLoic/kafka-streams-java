package io.conduktor.course.streams;

import io.conduktor.course.streams.avro.OilType;
import io.conduktor.course.streams.avro.RawTemperature;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.internals.TimeWindow;

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
