package io.conduktor.course.streams;

import static io.conduktor.course.streams.B_ProtoRecipeApp.ProvidedFunctions.*;

import io.conduktor.course.streams.proto.Order;
import io.conduktor.course.streams.proto.Order.OrderDetail.Dish;
import io.conduktor.course.streams.proto.Order.OrderDetail.Dish.Ingredient;
import io.conduktor.course.streams.proto.Sale;
import io.conduktor.course.streams.proto.Sale.Total;
import io.confluent.kafka.streams.serdes.protobuf.KafkaProtobufSerde;
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

public class B_ProtoRecipeApp {

  private static Logger logger = LoggerFactory.getLogger(B_ProtoRecipeApp.class);

  public static void main(String[] args) {

    Properties config = new Properties();

    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "recipe-processing-app");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

    final KafkaProtobufSerde<Sale> saleSerde = new KafkaProtobufSerde<>();

    final KafkaProtobufSerde<Order> orderSerde = new KafkaProtobufSerde<>();

    HashMap<String, Object> protSerdeConfig = new HashMap<>();

    protSerdeConfig.put("", "");
    protSerdeConfig.put("", "");
    protSerdeConfig.put("", "");

    saleSerde.configure(protSerdeConfig, false);
    orderSerde.configure(protSerdeConfig, false);

    final StreamsBuilder builder = new StreamsBuilder();

    final KStream<String, Order> orders =
        builder.stream("onsite-user-commands-proto", Consumed.with(Serdes.String(), orderSerde));

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

        .to("user-orders-proto", Produced.with(Serdes.String(), saleSerde));

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

  private static Sale orderToSale(
      final OrderWithNotesAccumulator<Order> order) {
    return Sale
        .newBuilder()
        .setUserId(order.getOrder().getUserId())
        .setOrderId(order.getOrder().getDetails().getId())
        .setTotal(ProvidedFunctions.computeTotal(order.getOrder()))
        .addAllComments(order.getComments())
        .build();
  }

  public static class ProvidedFunctions {

    private static Stream<Ingredient> extractIngredients(Order order) {
      return order
          .getDetails()
          .getDishesList()
          .stream()
          .flatMap(dish -> dish.getIngredientsList().stream());
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

    private static Optional<String> singleCurrencyOption(List<Dish> dishes) {
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

    private static Total computeTotal(final Order order) {
      final Total.Builder totalBuilder = Total.newBuilder();
      final List<Dish> dishes = order.getDetails().getDishesList();

      singleCurrencyOption(dishes).ifPresentOrElse(currency -> {
            totalBuilder
                .setValue(sumAllPrices(dishes))
                .setCurrency(currency);
          }, () ->
              totalBuilder
                  .setValue(0)
                  .setCurrency("Error: Multiple currencies found")

      );
      return totalBuilder.build();
    }
  }
}
