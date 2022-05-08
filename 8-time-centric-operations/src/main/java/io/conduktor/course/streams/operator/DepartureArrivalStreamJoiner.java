package io.conduktor.course.streams.operator;

import io.conduktor.course.streams.avro.Arrival;
import io.conduktor.course.streams.avro.Departure;
import io.conduktor.course.streams.avro.ShortTrip;
import java.time.Duration;
import java.time.ZoneId;
import org.apache.kafka.streams.kstream.ValueJoiner;

public class DepartureArrivalStreamJoiner implements ValueJoiner<Departure, Arrival, ShortTrip> {

  @Override
  public ShortTrip apply(final Departure value1, final Arrival value2) {
    return ShortTrip
        .newBuilder()
        .setVehicleId(value1.getVehicleId())
        .setStoreId(value2.getStoreId())
        .setTripDate(value1
            .getTimestamp()
            .atZone(ZoneId.of("UTC"))
            //.withZoneSameInstant(ZoneId.systemDefault())
            .toLocalDate()
        )
        .setDuration(
            Duration.between(
                value1.getTimestamp().atZone(ZoneId.of("UTC")),
                value2.getTimestamp().atZone(ZoneId.of("UTC"))
            ).getSeconds()
        ).setTripStartTime(value1.getTimestamp().atZone(ZoneId.of("UTC")).toLocalTime())
        .build();
  }
}
