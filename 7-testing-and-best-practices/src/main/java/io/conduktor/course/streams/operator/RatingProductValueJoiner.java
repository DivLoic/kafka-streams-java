package io.conduktor.course.streams.operator;

import io.conduktor.course.streams.avro.Product;
import io.conduktor.course.streams.avro.TotalScore;
import io.conduktor.course.streams.avro.ScoreDetail;
import org.apache.kafka.streams.kstream.ValueJoiner;

public class RatingProductValueJoiner
    implements ValueJoiner<TotalScore, Product, ScoreDetail> {

  @Override
  public ScoreDetail apply(final TotalScore rating, final Product product) {
    return ScoreDetail
        .newBuilder()
        .setName(product.getName())
        .setScore(rating.getScore())
        .build();
  }
}
