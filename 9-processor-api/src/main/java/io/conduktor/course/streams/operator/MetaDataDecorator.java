package io.conduktor.course.streams.operator;

import io.conduktor.course.streams.avro.OilType;
import io.conduktor.course.streams.avro.RawTemperature;
import io.conduktor.course.streams.avro.Temperature;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;

public class MetaDataDecorator implements ValueTransformer<RawTemperature, Temperature> {

  ProcessorContext context;

  private String extractHeader(String key) {
    return new String(context.headers().lastHeader(key).value(), StandardCharsets.UTF_8);
  }

  @Override
  public void init(final ProcessorContext context) {
    this.context = context;
  }

  @Override
  public Temperature transform(final RawTemperature value) {
    return Temperature
        .newBuilder()
        .setOilType(OilType.valueOf(extractHeader("oil_type")))
        .setOsVersion(extractHeader("os_version"))
        .setPrecision(extractHeader("precision"))
        .setFryer(value.getFryer())
        .setStore(value.getStore())
        .setTimestamp(value.getTimestamp())
        .setTemperature(value.getTemperature())
        .build();
  }

  @Override
  public void close() {

  }
}
