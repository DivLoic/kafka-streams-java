package io.conducktor.course.streams.operator;

import io.conducktor.course.streams.avro.Departure;
import java.util.Optional;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class DepartureTimestampExtractor implements TimestampExtractor {

  @Override
  public long extract(final ConsumerRecord<Object, Object> record, final long partitionTime) {
    return Optional
        .ofNullable((Departure) record.value())
        .map(departure -> departure.getTimestamp().toEpochMilli())
        .orElse(partitionTime);
  }
}
