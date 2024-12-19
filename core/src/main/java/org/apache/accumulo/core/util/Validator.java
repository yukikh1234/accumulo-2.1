
package org.apache.accumulo.core.util;

import static java.util.Objects.requireNonNull;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

public class Validator<T> {

  public static final Optional<String> OK = Optional.empty();

  private final Function<T,Optional<String>> validateFunction;
  private volatile T lastValidated = null;

  public Validator(final Function<T,Optional<String>> validateFunction) {
    this.validateFunction = requireNonNull(validateFunction);
  }

  public final T validate(final T argument) {
    if (isRecentlyValidated(argument)) {
      return argument;
    }

    validateFunction.apply(argument).ifPresent(this::throwValidationException);

    lastValidated = argument;
    return argument;
  }

  public final Validator<T> and(final Validator<T> other) {
    return (other == null) ? this : new Validator<>(
        arg -> validateFunction.apply(arg).or(() -> other.validateFunction.apply(arg)));
  }

  public final Validator<T> or(final Validator<T> other) {
    return (other == null) ? this : new Validator<>(
        arg -> validateFunction.apply(arg).isEmpty() ? OK : other.validateFunction.apply(arg));
  }

  public final Validator<T> not() {
    return new Validator<>(arg -> validateFunction.apply(arg).isPresent() ? OK
        : Optional.of("Validation should have failed with: Invalid argument " + arg));
  }

  private boolean isRecentlyValidated(T argument) {
    return lastValidated != null && Objects.equals(argument, lastValidated);
  }

  private void throwValidationException(String msg) {
    throw new IllegalArgumentException(msg);
  }
}
