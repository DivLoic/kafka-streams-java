package io.conduktor.course.streams;

import io.conduktor.course.streams.avro.Product;
import io.conduktor.course.streams.avro.Rating;
import io.conduktor.course.streams.avro.RatingAccumulator;
import io.conduktor.course.streams.avro.ScoreDetail;
import io.conduktor.course.streams.avro.TotalScore;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.state.KeyValueStore;

public class TopologyParams {

  Serde<Product> productSerde;
  Serde<Rating> ratingSerde;
  Serde<RatingAccumulator> ratingAccumulatorSerde;
  Serde<TotalScore> totalScoreSerde;
  Serde<ScoreDetail> scoreDetailSerde;

  public TopologyParams(
      final Serde<Product> product,
      final Serde<Rating> rating,
      final Serde<RatingAccumulator> ratingAccumulatorSerde,
      final Serde<TotalScore> totalScoreSerde,
      final Serde<ScoreDetail> scoreDetailSerde) {

    this.productSerde = product;
    this.ratingSerde = rating;
    this.ratingAccumulatorSerde = ratingAccumulatorSerde;
    this.totalScoreSerde = totalScoreSerde;
    this.scoreDetailSerde = scoreDetailSerde;
  }

  public Consumed<String, Product> getProductConsumed() {
    return Consumed.with(Serdes.String(), productSerde);
  }

  public Consumed<String, Product> getProductConsumed(String name) {
    return getProductConsumed().withName(name);
  }

  public Consumed<String, Rating> getRatingConsumed() {
    return Consumed.with(Serdes.String(), ratingSerde);
  }

  public Consumed<String, Rating> getRatingConsumed(String name) {
    return getRatingConsumed().withName(name);
  }

  public Grouped<String, Rating> getRatingGrouped() {
    return Grouped.with(Serdes.String(), ratingSerde);
  }

  public Grouped<String, Rating> getRatingGrouped(String name) {
    return getRatingGrouped().withName(name);
  }

  public Materialized<String, RatingAccumulator, KeyValueStore<Bytes, byte[]>> getRatingMaterialized(String name) {
    return Materialized
        .<String, RatingAccumulator, KeyValueStore<Bytes, byte[]>>as(name)
        .withKeySerde(Serdes.String())
        .withValueSerde(ratingAccumulatorSerde)
        .withCachingEnabled()
        .withLoggingDisabled();
  }

  public Joined<String, TotalScore, Product> getRatingProductJoined() {
    return Joined.with(Serdes.String(), totalScoreSerde, productSerde);
  }

  public Joined<String, TotalScore, Product> getRatingProductJoined(String name) {
    return getRatingProductJoined().withName(name);
  }

  public Repartitioned<String, TotalScore> getRatingRepartitioned() {
    return Repartitioned.with(Serdes.String(), totalScoreSerde);
  }

  public Repartitioned<String, TotalScore> getRatingRepartitioned(String name) {
    return getRatingRepartitioned().withName(name);
  }

  public Produced<String, ScoreDetail> getRatingProduced() {
    return Produced.with(Serdes.String(), scoreDetailSerde);
  }

  public Produced<String, ScoreDetail> getRatingProduced(String name) {
    return getRatingProduced().withName(name);
  }

}
