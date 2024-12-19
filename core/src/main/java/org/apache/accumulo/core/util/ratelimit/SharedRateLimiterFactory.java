
package org.apache.accumulo.core.util.ratelimit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.core.util.threads.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SharedRateLimiterFactory {
  private static final long REPORT_RATE = 60000;
  private static final long UPDATE_RATE = 1000;
  private static SharedRateLimiterFactory instance = null;
  private static ScheduledFuture<?> updateTaskFuture;
  private final Logger log = LoggerFactory.getLogger(SharedRateLimiterFactory.class);
  private final WeakHashMap<String,WeakReference<SharedRateLimiter>> activeLimiters =
      new WeakHashMap<>();

  private SharedRateLimiterFactory() {}

  public static synchronized SharedRateLimiterFactory getInstance(AccumuloConfiguration conf) {
    if (instance == null) {
      instance = new SharedRateLimiterFactory();
      initializeScheduledTasks(conf);
    }
    return instance;
  }

  private static void initializeScheduledTasks(AccumuloConfiguration conf) {
    ScheduledThreadPoolExecutor svc =
        ThreadPools.getServerThreadPools().createGeneralScheduledExecutorService(conf);
    updateTaskFuture = svc.scheduleWithFixedDelay(
        Threads.createNamedRunnable("SharedRateLimiterFactory update polling",
            SharedRateLimiterFactory::updateAll),
        UPDATE_RATE, UPDATE_RATE, MILLISECONDS);

    ScheduledFuture<?> future = svc.scheduleWithFixedDelay(
        Threads.createNamedRunnable("SharedRateLimiterFactory report polling",
            SharedRateLimiterFactory::reportAll),
        REPORT_RATE, REPORT_RATE, MILLISECONDS);
    ThreadPools.watchNonCriticalScheduledTask(future);
  }

  public interface RateProvider {
    long getDesiredRate();
  }

  public RateLimiter create(String name, RateProvider rateProvider) {
    synchronized (activeLimiters) {
      if (updateTaskFuture.isDone()) {
        log.warn("SharedRateLimiterFactory update task has failed.");
      }
      return activeLimiters.computeIfAbsent(name, k -> new WeakReference<>(
          new SharedRateLimiter(name, rateProvider, rateProvider.getDesiredRate()))).get();
    }
  }

  private void copyAndThen(String actionName, Consumer<SharedRateLimiter> action) {
    Map<String,SharedRateLimiter> limitersCopy = new HashMap<>();
    synchronized (activeLimiters) {
      activeLimiters.forEach((name, limiterRef) -> {
        SharedRateLimiter limiter = limiterRef.get();
        if (limiter != null) {
          limitersCopy.put(name, limiter);
        }
      });
    }
    limitersCopy.forEach((name, limiter) -> executeAction(actionName, action, name, limiter));
  }

  private void executeAction(String actionName, Consumer<SharedRateLimiter> action, String name,
      SharedRateLimiter limiter) {
    try {
      action.accept(limiter);
    } catch (RuntimeException e) {
      log.error("Failed to {} limiter {}", actionName, name, e);
    }
  }

  private static void updateAll() {
    instance.copyAndThen("update", SharedRateLimiter::update);
  }

  private static void reportAll() {
    instance.copyAndThen("report", SharedRateLimiter::report);
  }

  protected class SharedRateLimiter extends GuavaRateLimiter {
    private final AtomicLong permitsAcquired = new AtomicLong();
    private final AtomicLong lastUpdate = new AtomicLong();

    private final RateProvider rateProvider;
    private final String name;

    SharedRateLimiter(String name, RateProvider rateProvider, long initialRate) {
      super(initialRate);
      this.name = name;
      this.rateProvider = rateProvider;
      this.lastUpdate.set(System.nanoTime());
    }

    @Override
    public void acquire(long numPermits) {
      super.acquire(numPermits);
      permitsAcquired.addAndGet(numPermits);
    }

    public void update() {
      long rate = rateProvider.getDesiredRate();
      if (rate != getRate()) {
        setRate(rate);
      }
    }

    public void report() {
      if (log.isDebugEnabled()) {
        long duration = NANOSECONDS.toMillis(System.nanoTime() - lastUpdate.get());
        if (duration == 0) {
          return;
        }
        lastUpdate.set(System.nanoTime());

        long sum = permitsAcquired.get();
        permitsAcquired.set(0);

        if (sum > 0) {
          log.debug(String.format("RateLimiter '%s': %,d of %,d permits/second", name,
              sum * 1000L / duration, getRate()));
        }
      }
    }
  }
}
