package io.conducktor.course.streams;

import com.typesafe.config.Config;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.avro.specific.SpecificRecord;

public abstract class StreamingApp {

  Config config;

  protected Config getConfig() {
    return config;
  }

  protected Map<String, Object> getConfigMap() {
    return config
        .entrySet()
        .stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                entry -> entry.getValue().unwrapped().toString()
            )
        );
  }

  protected Properties getProperties() {
    Properties properties = new Properties();
    properties.putAll(getConfigMap());
    return properties;
  }

  protected <T extends SpecificRecord> SpecificAvroSerde<T> configuredAvroSerde() {
    final SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
    serde.configure(getConfigMap(), false);
    return serde;
  }
}
