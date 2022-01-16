package io.conducktor.course.streams;

import io.conducktor.course.streams.avro.Arrival;
import io.conducktor.course.streams.avro.Departure;
import io.conducktor.course.streams.avro.VehicleType;
import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneOffset;

public abstract class TestUtils {

  protected final Instant beginningOfTheDay(Instant time) {
    return time
        .atZone(ZoneOffset.UTC.normalized())
        .toLocalDate()
        .atTime(LocalTime.MIN)
        .toInstant(ZoneOffset.UTC);
  }
  protected final Instant beginningOfNextDay(Instant time) {
    return time
        .atZone(ZoneOffset.UTC.normalized())
        .toLocalDate()
        .plusDays(1)
        .atTime(LocalTime.MIN)
        .toInstant(ZoneOffset.UTC);
  }

  protected final Departure createTestDeparture(String vehicle, String Store, Instant time) {
    return Departure
        .newBuilder()
        .setVehicleId(vehicle)
        .setStoreId(Store)
        .setTimestamp(time)
        .setType(VehicleType.MOTORIZED)
        .build();
  }

  protected final Arrival createTestArrival(String vehicle, String Store, Instant time) {
    return Arrival
        .newBuilder()
        .setVehicleId(vehicle)
        .setStoreId(Store)
        .setTimestamp(time)
        .setType(VehicleType.MOTORIZED)
        .build();
  }
}
