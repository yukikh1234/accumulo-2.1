
package org.apache.accumulo.core.util;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A simple future wrapper that will set an atomic boolean to true if a future is successfully
 * canceled
 */
public class CancelFlagFuture<T> implements Future<T> {

  private final Future<T> wrappedFuture;
  private final AtomicBoolean cancelFlag;

  public CancelFlagFuture(Future<T> wrappedFuture, AtomicBoolean cancelFlag) {
    this.wrappedFuture = wrappedFuture;
    this.cancelFlag = cancelFlag;
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    if (wrappedFuture.cancel(mayInterruptIfRunning)) {
      cancelFlag.set(true);
      return true;
    }
    return false;
  }

  @Override
  public boolean isCancelled() {
    return wrappedFuture.isCancelled();
  }

  @Override
  public boolean isDone() {
    return wrappedFuture.isDone();
  }

  @Override
  public T get() throws InterruptedException, ExecutionException {
    return wrappedFuture.get();
  }

  @Override
  public T get(long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    return wrappedFuture.get(timeout, unit);
  }
}
