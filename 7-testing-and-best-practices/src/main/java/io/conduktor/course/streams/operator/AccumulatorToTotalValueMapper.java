package io.conduktor.course.streams.operator;

import io.conduktor.course.streams.avro.RatingAccumulator;
import io.conduktor.course.streams.avro.TotalScore;
import java.math.BigDecimal;
import java.math.RoundingMode;
import org.apache.kafka.streams.kstream.ValueMapper;

public class AccumulatorToTotalValueMapper implements ValueMapper<RatingAccumulator, TotalScore>  {

  private final int zero = 0;
  private final int rounding = 2;

  @Override
  public TotalScore apply(final RatingAccumulator accumulator) {
    Integer numberOfRating = accumulator.getScores().size();
    Integer sumOfAllRatings = accumulator.getScores().stream().reduce(zero, Integer::sum);

    final BigDecimal score = BigDecimal.valueOf(sumOfAllRatings)
        .divide(
            BigDecimal.valueOf(numberOfRating),
            rounding,
            RoundingMode.UP
        );

    return TotalScore
        .newBuilder()
        .setRatings(numberOfRating)
        .setScore(score.doubleValue())
        .build();
  }
}
