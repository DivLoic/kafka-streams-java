package io.conducktor.course.streams.generator;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.ShortSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Demo purpose: used to produce the data example. Do not edit this class.
 */
public class OrderProducer {

  private final static Logger logger = LoggerFactory.getLogger(OrderProducer.class);

  public static void main(String[] args) {

    final Integer input = Math.abs(Integer.parseInt(args[1]));
    final Integer order = Integer.valueOf(args[0]);
    final Short items = input.shortValue();

    Properties properties = new Properties();

    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ShortSerializer.class);
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.put(ProducerConfig.RETRIES_CONFIG, "0");
    properties.put(ProducerConfig.ACKS_CONFIG, "0");

    KafkaProducer<Integer, Short> producer = new KafkaProducer<>(properties);

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      logger.debug(String.format("Shunting down the generator: %s", OrderProducer.class));
      producer.flush();
      producer.close();
    }));

    logger.warn(String.format("Producing the order: %s -, number of items: %s", order, items));

    producer.send(new ProducerRecord<>("street-food-orders", order, items));
  }
}
