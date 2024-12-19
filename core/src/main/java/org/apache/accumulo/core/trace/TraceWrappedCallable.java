
/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
 * OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.trace;

import java.util.Objects;
import java.util.concurrent.Callable;

import org.apache.accumulo.core.util.threads.ThreadPools;

import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;

/**
 * A class to wrap {@link Callable}s for {@link ThreadPools} in a way that still provides access to
 * the wrapped {@link Callable} instance. This supersedes the use of {@link Context#wrap(Callable)}.
 *
 * @param <V> the result type of method call
 */
class TraceWrappedCallable<V> implements Callable<V> {

  private final Context context;
  private final Callable<V> unwrapped;

  /**
   * Recursively unwraps a {@link Callable} that might be wrapped inside multiple layers of
   * {@link TraceWrappedCallable}.
   *
   * @param c the {@link Callable} to unwrap
   * @param <C> the result type of the callable
   * @return the innermost wrapped {@link Callable}
   */
  static <C> Callable<C> unwrapFully(Callable<C> c) {
    while (c instanceof TraceWrappedCallable) {
      c = ((TraceWrappedCallable<C>) c).unwrapped;
    }
    return c;
  }

  /**
   * Constructs a {@link TraceWrappedCallable} and unwraps the provided {@link Callable}.
   *
   * @param other the {@link Callable} to be wrapped
   */
  TraceWrappedCallable(Callable<V> other) {
    this.context = Context.current();
    this.unwrapped = unwrapFully(other);
  }

  /**
   * Executes the wrapped {@link Callable} within the current context.
   *
   * @return the result of the wrapped {@link Callable} call
   * @throws Exception if the wrapped callable throws an exception
   */
  @Override
  public V call() throws Exception {
    try (Scope unused = context.makeCurrent()) {
      return unwrapped.call();
    }
  }

  /**
   * Determines whether the specified object is equal to the current instance.
   *
   * @param obj the object to compare with the current instance
   * @return true if the specified object is equal to the current instance; false otherwise
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof TraceWrappedCallable)) {
      return false;
    }
    TraceWrappedCallable<?> other = (TraceWrappedCallable<?>) obj;
    return Objects.equals(unwrapped, other.unwrapped);
  }

  /**
   * Returns the hash code for the current instance.
   *
   * @return the hash code of the wrapped {@link Callable}
   */
  @Override
  public int hashCode() {
    return Objects.hashCode(unwrapped);
  }
}
