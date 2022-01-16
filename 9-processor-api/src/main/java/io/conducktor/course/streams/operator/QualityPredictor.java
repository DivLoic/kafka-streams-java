package io.conducktor.course.streams.operator;

import io.conducktor.course.streams.avro.QualityWarning;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.Set;
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
    BigDecimal slope = BigDecimal.valueOf(0.5);
    BigDecimal origin = BigDecimal.valueOf(0.5);
    BigDecimal max =
        BigDecimal.valueOf(Instant.now().getEpochSecond() - starTime.getEpochSecond());

    return slope.multiply(max).add(origin).doubleValue();
  }

  @Override
  public void init(final ProcessorContext<String, QualityWarning> context) {
    this.context = context;
    stateStore = context.getStateStore("max-temperature-sessions");
    context.schedule(Duration.ofSeconds(5), PunctuationType.WALL_CLOCK_TIME, timestamp -> {

      System.out.println("punctuating");

      Set<String> keys = new HashSet<>();
      final KeyValueIterator<Windowed<String>, Long> sessions =
          this.stateStore.fetch("KS90001");//.fetch("KS90000", "KS99999");

      sessions.forEachRemaining((kv) -> {
        System.out.println(String.format("checking %s / %s - %s : %s",
            kv.key.key(),
            kv.key.window().startTime().atZone(ZoneOffset.UTC.normalized()),
            kv.key.window().endTime().atZone(ZoneOffset.UTC.normalized()),
            kv.value
        ));


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
