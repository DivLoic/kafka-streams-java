package io.conduktor.course.streams;


import io.conduktor.course.streams.avro.QualityWarning;
import io.conduktor.course.streams.avro.RawTemperature;
import io.conduktor.course.streams.avro.Temperature;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.SessionStore;

public class TopologyParams {

  private Serde<RawTemperature> rawTemperatureSerde;
  private Serde<Temperature> temperatureSerde;
  private Serde<QualityWarning> qualityWarningSerde;

  public TopologyParams(
      final Serde<RawTemperature> rawTemperatureSerde,
      final Serde<Temperature> temperatureSerde,
      final Serde<QualityWarning> qualityWarningSerde) {
    this.rawTemperatureSerde = rawTemperatureSerde;
    this.temperatureSerde = temperatureSerde;
    this.qualityWarningSerde = qualityWarningSerde;
  }

  public Consumed<String, RawTemperature> getRawTemperatureConsumed() {
    return Consumed.with(Serdes.String(), this.rawTemperatureSerde);
  }

  public Serializer<QualityWarning> getQualityWarningSerializer() {
    return this.qualityWarningSerde.serializer();
  }

  public Deserializer<QualityWarning> getQualityWarningDeserializer() {
    return this.qualityWarningSerde.deserializer();
  }

  public Grouped<String, Temperature> getTemperatureGrouped() {
    return Grouped.with(Serdes.String(), this.temperatureSerde);
  }

  public Grouped<String, Temperature> getTemperatureGrouped(String name) {
    return getTemperatureGrouped().withName(name);
  }

  public Materialized<String, Long, SessionStore<Bytes, byte[]>> getMaxTemperatureMaterialized() {
    return Materialized.with(Serdes.String(), Serdes.Long());
  }

  public Materialized<String, Long, SessionStore<Bytes, byte[]>> getMaxTemperatureMaterialized(
      String name) {
    return getMaxTemperatureMaterialized().as("max-temperature-sessions");
  }


}
