package io.conducktor.course.streams;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class OrderWithNotesAccumulator<T> {

  private T order;
  private List<String> comments;

  public T getOrder() {
    return order;
  }

  public List<String> getComments() {
    return comments;
  }

  public OrderWithNotesAccumulator(T order) {
    this.order = order;
    this.comments = Collections.emptyList();
  }

  public OrderWithNotesAccumulator(T order, List<String> comments) {
    this.order = order;
    this.comments = Collections.unmodifiableList(comments);
  }

  public OrderWithNotesAccumulator<T> add(String comment) {
    return new OrderWithNotesAccumulator<>(
        this.order,
        Stream
            .concat(Stream.of(comment), this.comments.stream())
            .collect(Collectors.toList())
    );
  }
}
