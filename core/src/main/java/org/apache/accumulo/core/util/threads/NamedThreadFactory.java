
package org.apache.accumulo.core.util.threads;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.OptionalInt;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ThreadFactory that sets the name and optionally the priority on a newly created Thread.
 */
class NamedThreadFactory implements ThreadFactory {

  private static final String FORMAT = "%s-%s-%d";

  private final AtomicInteger threadNum = new AtomicInteger(1);
  private final String name;
  private final OptionalInt priority;
  private final UncaughtExceptionHandler handler;

  NamedThreadFactory(String name, UncaughtExceptionHandler ueh) {
    this(name, OptionalInt.empty(), ueh);
  }

  NamedThreadFactory(String name, OptionalInt priority, UncaughtExceptionHandler ueh) {
    this.name = name;
    this.priority = priority;
    this.handler = ueh;
  }

  @Override
  public Thread newThread(Runnable r) {
    String threadName = generateThreadName(r);
    return Threads.createThread(threadName, priority, r, handler);
  }

  private String generateThreadName(Runnable r) {
    if (r instanceof NamedRunnable) {
      NamedRunnable nr = (NamedRunnable) r;
      return String.format(FORMAT, name, nr.getName(), threadNum.getAndIncrement());
    } else {
      return String.format(FORMAT, name, r.getClass().getSimpleName(), threadNum.getAndIncrement());
    }
  }
}
