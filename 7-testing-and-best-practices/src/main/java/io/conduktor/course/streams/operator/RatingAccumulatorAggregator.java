package io.conduktor.course.streams.operator;

import io.conduktor.course.streams.avro.Rating;
import io.conduktor.course.streams.avro.RatingAccumulator;
import java.util.List;
import org.apache.kafka.streams.kstream.Aggregator;

public class RatingAccumulatorAggregator implements Aggregator<String, Rating, RatingAccumulator> {

  @Override
  public RatingAccumulator apply(final String key, final Rating rating,
                                 final RatingAccumulator aggregate) {
    final List<Integer> scores = aggregate.getScores();
    scores.add(rating.getScore());

    return RatingAccumulator
        .newBuilder()
        .setScores(scores)
        .build();
  }
}
