package io.conduktor.course.streams;

import static io.conduktor.course.streams.C_JsonRecipeApp.ProvidedFunctions.computeTotal;
import static io.conduktor.course.streams.C_JsonRecipeApp.ProvidedFunctions.hasNuts;
import static io.conduktor.course.streams.C_JsonRecipeApp.ProvidedFunctions.isFresh;
import static io.conduktor.course.streams.C_JsonRecipeApp.ProvidedFunctions.isHot;

import io.conduktor.course.streams.json.Dish;
import io.conduktor.course.streams.json.Ingredient;
import io.conduktor.course.streams.json.Order;
import io.conduktor.course.streams.json.Sale;
import io.conduktor.course.streams.json.Total;
import io.confluent.kafka.streams.serdes.json.KafkaJsonSchemaSerde;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class C_JsonRecipeApp {

  private static Logger logger = LoggerFactory.getLogger(C_JsonRecipeApp.class);

  public static void main(String[] args) {

    Properties config = new Properties();

    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "recipe-processing-app");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

    final KafkaJsonSchemaSerde<Sale> saleSerde =
        new KafkaJsonSchemaSerde<>();

    final KafkaJsonSchemaSerde<Order> orderSerde =
        new KafkaJsonSchemaSerde<>();

    HashMap<String, Object> jsonSerdeConfig = new HashMap<>();

    jsonSerdeConfig.put("", "");
    jsonSerdeConfig.put("", "");
    jsonSerdeConfig.put("", "");

    saleSerde.configure(jsonSerdeConfig,false);
    orderSerde.configure(jsonSerdeConfig,false);

    final StreamsBuilder builder = new StreamsBuilder();

    final KStream<String, Order> orders =
        builder.stream("onsite-user-commands-json", Consumed.with(Serdes.String(), orderSerde));

    orders

        .filter(($, order) -> isAValidOnSiteOrder(order))

        .mapValues(order -> {

          final OrderWithNotesAccumulator<Order> orderWithComments =
              isFresh
                  .andThen(isHot)
                  .andThen(hasNuts)
                  .apply(new OrderWithNotesAccumulator<>(order));

          return orderToSale(orderWithComments);
        })

        .to("user-orders-json", Produced.with(Serdes.String(), saleSerde));

    Topology topology = builder.build();
    KafkaStreams streams = new KafkaStreams(topology, config);

    TopologyDescription description = topology.describe();
    logger.info(description.toString());

    streams.start();

    // shutdown hook to correctly close the streaming application
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }

  private static boolean isAValidOnSiteOrder(final Order order) {
    return Optional.ofNullable(order.getPayment()).isEmpty();
  }

  private static Sale orderToSale(final OrderWithNotesAccumulator<Order> order) {
    return new Sale()
        .withUserId(order.getOrder().getUserId())
        .withOrderId(order.getOrder().getDetails().getId())
        .withTotal(computeTotal(order.getOrder()))
        .withComments(order.getComments());
  }

  public static class ProvidedFunctions {

    private static Stream<Ingredient> extractIngredients(
        Order order) {
      return order
          .getDetails()
          .getDishes()
          .stream()
          .flatMap(dish -> dish.getIngredients().stream());
    }

    public static Function<OrderWithNotesAccumulator<Order>, OrderWithNotesAccumulator<Order>>
        isFresh = order -> {
      final Stream<Ingredient> ingredients = extractIngredients(order.getOrder());
      if (ingredients.anyMatch(ingredient -> ingredient.getName().equals("egg"))) {
        return order.add("/!\\ warning: this order's recipes contains fresh eggs.");
      } else {
        return order;
      }
    };

    public static Function<OrderWithNotesAccumulator<Order>, OrderWithNotesAccumulator<Order>>
        isHot = order -> {
      final Stream<Ingredient> ingredients = extractIngredients(order.getOrder());
      if (ingredients.anyMatch(ingredient -> ingredient.getName().equals("chilli"))) {
        return order.add("/!\\ warning: this order's recipes are very spicy.");
      } else {
        return order;
      }

    };

    public static Function<OrderWithNotesAccumulator<Order>, OrderWithNotesAccumulator<Order>>
        hasNuts = order -> {
      final Stream<Ingredient> ingredients = extractIngredients(order.getOrder());

      final boolean hasARecipeContainingTreeNuts = ingredients
          .anyMatch(ingredient -> Stream.of("Hazelnuts", "Almonds", "Pecans")
              .anyMatch(nuts -> ingredient.getName().equals(nuts)));

      if (hasARecipeContainingTreeNuts) {
        return order.add("/!\\ warning: this order's recipes contain tree nuts.");
      } else {
        return order;
      }
    };

    private static Optional<String> singleCurrencyOption(
        List<Dish> dishes) {
      final Set<String> currencies = dishes
          .stream().map(dish -> dish.getPrice().getCurrency())
          .collect(Collectors.toSet());

      if (currencies.size() == 1) {
        return currencies.stream().findFirst();
      } else {
        return Optional.empty();
      }
    }

    private static Integer sumAllPrices(List<Dish> dishes) {
      return dishes.stream().map(dish -> dish.getPrice().getValue() * dish.getQuantity())
          .reduce(Integer::sum).orElse(0);
    }

    public static Total computeTotal(final Order order) {
      final Total total = new Total();
      final List<Dish> dishes = order.getDetails().getDishes();

      singleCurrencyOption(dishes).ifPresentOrElse(currency -> {
        total
                .withValue(sumAllPrices(dishes))
                .withCurrency(currency);
          }, () ->
          total
                  .withValue(0)
                  .withCurrency("Error: Multiple currencies found")

      );
      return total;
    }
  }
}
