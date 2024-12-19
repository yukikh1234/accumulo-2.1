
package org.apache.accumulo.core.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class CompletableFutureUtil {

  // Create a binary tree of completable future operations, where each node in the tree merges the
  // results of their children when complete
  public static <T> CompletableFuture<T> merge(List<CompletableFuture<T>> futures,
      BiFunction<T,T,T> mergeFunc, Supplier<T> nothing) {
    if (futures.isEmpty()) {
      return CompletableFuture.completedFuture(nothing.get());
    }
    // Break down the merge logic into smaller methods for readability
    while (futures.size() > 1) {
      futures = mergeFutures(futures, mergeFunc);
    }
    return futures.get(0);
  }

  // Helper method to merge futures in pairs
  private static <T> List<CompletableFuture<T>> mergeFutures(List<CompletableFuture<T>> futures,
      BiFunction<T,T,T> mergeFunc) {
    ArrayList<CompletableFuture<T>> mergedFutures = new ArrayList<>(futures.size() / 2);
    for (int i = 0; i < futures.size(); i += 2) {
      mergedFutures.add(mergePair(futures, i, mergeFunc));
    }
    return mergedFutures;
  }

  // Helper method to merge a pair of futures
  private static <T> CompletableFuture<T> mergePair(List<CompletableFuture<T>> futures, int i,
      BiFunction<T,T,T> mergeFunc) {
    if (i + 1 == futures.size()) {
      return futures.get(i);
    } else {
      return futures.get(i).thenCombine(futures.get(i + 1), mergeFunc);
    }
  }

  /**
   * Iterate some function until a given condition is met.
   *
   * The step function should always return an asynchronous {@code CompletableFuture} in order to
   * avoid stack overflows.
   */
  public static <T> CompletableFuture<T> iterateUntil(Function<T,CompletableFuture<T>> step,
      Predicate<T> isDone, T init) {
    Function<T,CompletableFuture<T>> recursiveFunction = new Function<>() {
      @Override
      public CompletableFuture<T> apply(T x) {
        if (isDone.test(x)) {
          return CompletableFuture.completedFuture(x);
        }
        return step.apply(x).thenCompose(this);
      }
    };
    return CompletableFuture.completedFuture(init).thenCompose(recursiveFunction);
  }
}
