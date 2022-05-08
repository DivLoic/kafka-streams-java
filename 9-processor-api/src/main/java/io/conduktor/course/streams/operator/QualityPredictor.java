package io.conduktor.course.streams.operator;

import io.conduktor.course.streams.avro.QualityWarning;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.SessionStore;

public class QualityPredictor implements Processor<Windowed<String>, Long, String, QualityWarning> {

  ProcessorContext<String, QualityWarning> context;
  SessionStore<String, Long> stateStore;

  public double predict(Instant starTime) {
    final Instant now = Instant.now();
    BigDecimal slope = BigDecimal.valueOf(0.2);
    BigDecimal origin = BigDecimal.valueOf(10);

    BigDecimal uptime =
        BigDecimal.valueOf(Duration.between(starTime, now).toMinutes());

    System.out.printf("now %s, starTime %s, uptime %s m%n", now, starTime, uptime);

    return slope.multiply(uptime).add(origin).doubleValue();
  }

  @Override
  public void init(final ProcessorContext<String, QualityWarning> context) {
    this.context = context;
    stateStore = context.getStateStore("max-temperature-sessions");
    context.schedule(Duration.ofSeconds(5), PunctuationType.WALL_CLOCK_TIME, timestamp -> {

      System.out.println("punctuating");

      Set<String> keys = new HashSet<>();
      final KeyValueIterator<Windowed<String>, Long> sessions =
          this.stateStore.fetch("F901", "F999");

      sessions.forEachRemaining((kv) -> {
        System.out.println(String.format("checking %s / %s - %s : %s",
            kv.key.key(),
            kv.key.window().startTime().atZone(ZoneOffset.UTC.normalized()),
            kv.key.window().endTime().atZone(ZoneOffset.UTC.normalized()),
            kv.value
        ));

        context.forward(new Record<>(
                kv.key.key(),
                QualityWarning
                    .newBuilder()
                    .setSession(kv.key.window().startTime())
                    .setTpmPrediction(String.valueOf(predict(kv.key.window().startTime()))).build(),
                Instant.now().toEpochMilli()),
            "produce-warning");


                /*if (!keys.contains(kv.key.key())) {
                  System.out.println(String.format("oil quality prediction: %s - %s ",
                      kv.key.key(), predict(kv.key.window().startTime())));
                  keys.add(kv.key.key());
                }*/

      });

      sessions.close();
    });
  }

  @Override
  public void process(final Record<Windowed<String>, Long> record) {
    System.out.println(String.format("Final value for %s / %s - %s : %s",
        record.key().key(),
        record.key().window().startTime().atZone(ZoneOffset.UTC.normalized()),
        record.key().window().endTime().atZone(ZoneOffset.UTC.normalized()),
        record.value()
    ));

    stateStore.remove(record.key());

  }

  @Override
  public void close() {

  }
}
