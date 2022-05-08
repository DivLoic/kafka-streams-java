package io.conduktor.course.streams.operator;

import io.conduktor.course.streams.avro.Arrival;
import java.time.Instant;
import java.util.Optional;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class ArrivalTimestampExtractor implements TimestampExtractor {

  @Override
  public long extract(final ConsumerRecord<Object, Object> record, final long partitionTime) {
    return Optional
        .ofNullable((Arrival) record.value())
        .map(arrival -> arrival.getTimestamp().toEpochMilli())
        .orElse(partitionTime);
  }
}
