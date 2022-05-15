package io.conduktor.course.streams;

import io.conduktor.course.streams.avro.Arrival;
import io.conduktor.course.streams.avro.Departure;
import io.conduktor.course.streams.avro.ShortTrip;
import io.conduktor.course.streams.operator.ArrivalTimestampExtractor;
import io.conduktor.course.streams.operator.DepartureTimestampExtractor;
import java.time.Duration;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.state.WindowStore;

public class TopologyParams {

  private Serde<Departure> departureSerde;
  private Serde<Arrival> arrivalSerde;
  private Serde<ShortTrip> shortTripSerde;

  public TopologyParams(
      Serde<Departure> departureSerde,
      Serde<Arrival> arrivalSerde,
      Serde<ShortTrip> shortTripSerde
  ) {
    this.departureSerde = departureSerde;
    this.arrivalSerde = arrivalSerde;
    this.shortTripSerde = shortTripSerde;
  }

  public Consumed<String, Departure> getDepartureConsumed() {
    return Consumed.with(Serdes.String(), departureSerde)
        .withTimestampExtractor(new DepartureTimestampExtractor());
  }

  public Consumed<String, Departure> getDepartureConsumed(String name) {
    return getDepartureConsumed().withName(name);
  }

  public Consumed<String, Arrival> getArrivalConsumed() {
    return Consumed.with(Serdes.String(), arrivalSerde)
        .withTimestampExtractor(new ArrivalTimestampExtractor());
  }

  public Consumed<String, Arrival> getArrivalConsumed(String name) {
    return getArrivalConsumed().withName(name);
  }

  public StreamJoined<String, Departure, Arrival> getDepartureArrivalStreamJoined() {
    return StreamJoined.with(Serdes.String(), departureSerde, arrivalSerde);
  }

  public StreamJoined<String, Departure, Arrival> getDepartureArrivalStreamJoined(String name) {
    return getDepartureArrivalStreamJoined().withName(name);
  }

  public Grouped<String, ShortTrip> getShortTripCountGrouped() {
    return Grouped.with(Serdes.String(), shortTripSerde);
  }

  static public Materialized<String, Long, WindowStore<Bytes, byte[]>> getShortTripCountMaterialized() {
    return Materialized.with(Serdes.String(), Serdes.Long());
  }

  static public Materialized<String, Long, WindowStore<Bytes, byte[]>> getShortTripCountMaterialized(
      String name) {
    return getShortTripCountMaterialized().as(name);
  }

  static public Produced<Windowed<String>, Long> getWindowedCountProduced(Duration windowLength) {
    return Produced.with(
        WindowedSerdes.timeWindowedSerdeFrom(String.class, windowLength.toMillis()),
        Serdes.Long()
    );
  }

  static public Produced<Windowed<String>, Long> getWindowedCountProduced(Duration windowLength,
                                                                          String name) {
    return getWindowedCountProduced(windowLength).withName(name);
  }


}
