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
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.accumulo.core.util.threads;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.accumulo.core.util.threads.ThreadPoolNames.*;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.Iterator;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.*;
import java.util.function.IntSupplier;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.trace.TraceUtil;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;

@SuppressFBWarnings(value = "RV_EXCEPTION_NOT_THROWN",
    justification = "Throwing Error for it to be caught by AccumuloUncaughtExceptionHandler")
public class ThreadPools {

  public static class ExecutionError extends Error {
    private static final long serialVersionUID = 1L;

    public ExecutionError(String message, Throwable cause) {
      super(message, cause);
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(ThreadPools.class);
  public static final long DEFAULT_TIMEOUT_MILLISECS = MINUTES.toMillis(3);
  private static final ThreadPools SERVER_INSTANCE = new ThreadPools(Threads.UEH);

  public static final ThreadPools getServerThreadPools() {
    return SERVER_INSTANCE;
  }

  public static final ThreadPools getClientThreadPools(UncaughtExceptionHandler ueh) {
    return new ThreadPools(ueh);
  }

  private static final ThreadPoolExecutor SCHEDULED_FUTURE_CHECKER_POOL =
      getServerThreadPools().getPoolBuilder(SCHED_FUTURE_CHECKER_POOL).numCoreThreads(1).build();

  private static final ConcurrentLinkedQueue<ScheduledFuture<?>> CRITICAL_RUNNING_TASKS =
      new ConcurrentLinkedQueue<>();

  private static final ConcurrentLinkedQueue<ScheduledFuture<?>> NON_CRITICAL_RUNNING_TASKS =
      new ConcurrentLinkedQueue<>();

  private static final Runnable TASK_CHECKER = () -> {
    final List<ConcurrentLinkedQueue<ScheduledFuture<?>>> queues =
        List.of(CRITICAL_RUNNING_TASKS, NON_CRITICAL_RUNNING_TASKS);
    while (true) {
      queues.forEach(q -> {
        Iterator<ScheduledFuture<?>> tasks = q.iterator();
        while (tasks.hasNext()) {
          if (checkTaskFailed(tasks.next(), q)) {
            tasks.remove();
          }
        }
      });
      try {
        MINUTES.sleep(1);
      } catch (InterruptedException ie) {
        Thread.interrupted();
      }
    }
  };

  private static boolean checkTaskFailed(ScheduledFuture<?> future,
      ConcurrentLinkedQueue<ScheduledFuture<?>> taskQueue) {
    if (future.isDone()) {
      return handleTaskCompletion(future, taskQueue);
    }
    return false;
  }

  private static boolean handleTaskCompletion(ScheduledFuture<?> future,
      ConcurrentLinkedQueue<ScheduledFuture<?>> taskQueue) {
    try {
      future.get();
      return true;
    } catch (ExecutionException ee) {
      handleExecutionException(taskQueue, ee);
      return true;
    } catch (CancellationException ce) {
      return true;
    } catch (InterruptedException ie) {
      LOG.info("Interrupted while waiting to check on scheduled background task.");
      Thread.interrupted();
    }
    return false;
  }

  private static void handleExecutionException(ConcurrentLinkedQueue<ScheduledFuture<?>> taskQueue,
      ExecutionException ee) {
    if (taskQueue == CRITICAL_RUNNING_TASKS) {
      throw new ExecutionError("Critical scheduled background task failed.", ee);
    } else {
      LOG.error("Non-critical scheduled background task failed", ee);
    }
  }

  static {
    SCHEDULED_FUTURE_CHECKER_POOL.execute(TASK_CHECKER);
  }

  public static void watchCriticalScheduledTask(ScheduledFuture<?> future) {
    CRITICAL_RUNNING_TASKS.add(future);
  }

  public static void watchCriticalFixedDelay(AccumuloConfiguration aconf, long intervalMillis,
      Runnable runnable) {
    ScheduledFuture<?> future = getServerThreadPools().createGeneralScheduledExecutorService(aconf)
        .scheduleWithFixedDelay(runnable, intervalMillis, intervalMillis, TimeUnit.MILLISECONDS);
    CRITICAL_RUNNING_TASKS.add(future);
  }

  public static void watchNonCriticalScheduledTask(ScheduledFuture<?> future) {
    NON_CRITICAL_RUNNING_TASKS.add(future);
  }

  public static void ensureRunning(ScheduledFuture<?> future, String message) {
    if (future.isDone()) {
      handleEnsureRunning(future, message);
    }
  }

  private static void handleEnsureRunning(ScheduledFuture<?> future, String message) {
    try {
      future.get();
    } catch (Exception e) {
      throw new IllegalStateException(message, e);
    }
    throw new IllegalStateException(message);
  }

  public static void resizePool(final ThreadPoolExecutor pool, final IntSupplier maxThreads,
      String poolName) {
    int count = pool.getMaximumPoolSize();
    int newCount = maxThreads.getAsInt();
    if (count != newCount) {
      LOG.info("Changing max threads for {} from {} to {}", poolName, count, newCount);
      adjustPoolSize(pool, count, newCount);
    }
  }

  private static void adjustPoolSize(ThreadPoolExecutor pool, int count, int newCount) {
    if (newCount > count) {
      pool.setMaximumPoolSize(newCount);
      pool.setCorePoolSize(newCount);
    } else {
      pool.setCorePoolSize(newCount);
      pool.setMaximumPoolSize(newCount);
    }
  }

  public static void resizePool(final ThreadPoolExecutor pool, final AccumuloConfiguration conf,
      final Property p) {
    resizePool(pool, () -> conf.getCount(p), p.getKey());
  }

  private final UncaughtExceptionHandler handler;

  private ThreadPools(UncaughtExceptionHandler ueh) {
    handler = ueh;
  }

  public ThreadPoolExecutor createExecutorService(final AccumuloConfiguration conf,
      final Property p) {
    return createExecutorService(conf, p, false);
  }

  public ThreadPoolExecutor createExecutorService(final AccumuloConfiguration conf,
      final Property p, boolean emitThreadPoolMetrics) {
    ThreadPoolExecutorBuilder builder;
    switch (p) {
      case GENERAL_SIMPLETIMER_THREADPOOL_SIZE:
        return createScheduledExecutorService(conf.getCount(p),
            GENERAL_SERVER_SIMPLETIMER_POOL.poolName);
      case GENERAL_THREADPOOL_SIZE:
        return createScheduledExecutorService(conf.getCount(p), GENERAL_SERVER_POOL.poolName,
            emitThreadPoolMetrics);
      default:
        return createSpecificExecutorService(conf, p, emitThreadPoolMetrics);
    }
  }

  private ThreadPoolExecutor createSpecificExecutorService(final AccumuloConfiguration conf,
      final Property p, boolean emitThreadPoolMetrics) {
    ThreadPoolExecutorBuilder builder;
    switch (p) {
      case MANAGER_BULK_THREADPOOL_SIZE:
        builder =
            getPoolBuilder(MANAGER_BULK_IMPORT_POOL).numCoreThreads(conf.getCount(p)).withTimeOut(
                conf.getTimeInMillis(Property.MANAGER_BULK_THREADPOOL_TIMEOUT), MILLISECONDS);
        break;
      case MANAGER_RENAME_THREADS:
        builder = getPoolBuilder(MANAGER_RENAME_POOL).numCoreThreads(conf.getCount(p));
        break;
      case MANAGER_FATE_THREADPOOL_SIZE:
        builder = getPoolBuilder(MANAGER_FATE_POOL).numCoreThreads(conf.getCount(p));
        break;
      case MANAGER_STATUS_THREAD_POOL_SIZE:
        builder = getPoolBuilder(MANAGER_STATUS_POOL);
        configureStatusThreadPool(conf, p, builder);
        break;
      case TSERV_WORKQ_THREADS:
        builder = getPoolBuilder(TSERVER_WORKQ_POOL).numCoreThreads(conf.getCount(p));
        break;
      case TSERV_MINC_MAXCONCURRENT:
        builder = getPoolBuilder(TSERVER_MINOR_COMPACTOR_POOL).numCoreThreads(conf.getCount(p))
            .withTimeOut(0L, MILLISECONDS);
        break;
      case TSERV_MIGRATE_MAXCONCURRENT:
        builder = getPoolBuilder(TSERVER_MIGRATIONS_POOL).numCoreThreads(conf.getCount(p))
            .withTimeOut(0L, MILLISECONDS);
        break;
      case TSERV_ASSIGNMENT_MAXCONCURRENT:
        builder = getPoolBuilder(TSERVER_ASSIGNMENT_POOL).numCoreThreads(conf.getCount(p))
            .withTimeOut(0L, MILLISECONDS);
        break;
      case TSERV_SUMMARY_RETRIEVAL_THREADS:
        builder = getPoolBuilder(TSERVER_SUMMARY_RETRIEVAL_POOL).numCoreThreads(conf.getCount(p))
            .withTimeOut(60L, MILLISECONDS);
        break;
      case TSERV_SUMMARY_REMOTE_THREADS:
        builder = getPoolBuilder(TSERVER_SUMMARY_REMOTE_POOL).numCoreThreads(conf.getCount(p))
            .withTimeOut(60L, MILLISECONDS);
        break;
      case TSERV_SUMMARY_PARTITION_THREADS:
        builder = getPoolBuilder(TSERVER_SUMMARY_PARTITION_POOL).numCoreThreads(conf.getCount(p))
            .withTimeOut(60L, MILLISECONDS);
        break;
      case GC_DELETE_THREADS:
        return getPoolBuilder(GC_DELETE_POOL).numCoreThreads(conf.getCount(p)).build();
      case REPLICATION_WORKER_THREADS:
        builder = getPoolBuilder(REPLICATION_WORKER_POOL).numCoreThreads(conf.getCount(p));
        break;
      default:
        throw new IllegalArgumentException("Unhandled thread pool property: " + p);
    }
    if (emitThreadPoolMetrics) {
      builder.enableThreadPoolMetrics();
    }
    return builder.build();
  }

  private void configureStatusThreadPool(final AccumuloConfiguration conf, final Property p,
      ThreadPoolExecutorBuilder builder) {
    int threads = conf.getCount(p);
    if (threads == 0) {
      builder.numCoreThreads(0).numMaxThreads(Integer.MAX_VALUE).withTimeOut(60L, SECONDS)
          .withQueue(new SynchronousQueue<>());
    } else {
      builder.numCoreThreads(threads);
    }
  }

  public ThreadPoolExecutorBuilder getPoolBuilder(@NonNull final ThreadPoolNames pool) {
    return new ThreadPoolExecutorBuilder(pool.poolName);
  }

  public ThreadPoolExecutorBuilder getPoolBuilder(@NonNull final String name) {
    String trimmed = name.trim();
    String poolName = trimmed.startsWith(ACCUMULO_POOL_PREFIX.poolName) ? trimmed
        : ACCUMULO_POOL_PREFIX.poolName + (trimmed.startsWith(".") ? trimmed : "." + trimmed);
    return new ThreadPoolExecutorBuilder(poolName);
  }

  public class ThreadPoolExecutorBuilder {
    final String name;
    int coreThreads = 0;
    int maxThreads = -1;
    long timeOut = DEFAULT_TIMEOUT_MILLISECS;
    TimeUnit units = MILLISECONDS;
    BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();
    OptionalInt priority = OptionalInt.empty();
    boolean emitThreadPoolMetrics = false;

    ThreadPoolExecutorBuilder(@NonNull final String name) {
      this.name = name;
    }

    public ThreadPoolExecutor build() {
      validateBuilder();
      return createThreadPool(coreThreads, maxThreads, timeOut, units, name, queue, priority,
          emitThreadPoolMetrics);
    }

    private void validateBuilder() {
      Preconditions.checkArgument(coreThreads >= 0,
          "The number of core threads must be 0 or larger");
      if (maxThreads < 0) {
        maxThreads = coreThreads == 0 ? 1 : coreThreads;
      }
      Preconditions.checkArgument(maxThreads >= coreThreads,
          "The number of max threads must be greater than 0 and greater than or equal to the number of core threads");
      Preconditions.checkArgument(
          priority.orElse(1) >= Thread.MIN_PRIORITY && priority.orElse(1) <= Thread.MAX_PRIORITY,
          "invalid thread priority, range must be Thread.MIN_PRIORITY <= priority <= Thread.MAX_PRIORITY");
    }

    public ThreadPoolExecutorBuilder numCoreThreads(int coreThreads) {
      this.coreThreads = coreThreads;
      return this;
    }

    public ThreadPoolExecutorBuilder numMaxThreads(int maxThreads) {
      this.maxThreads = maxThreads;
      return this;
    }

    public ThreadPoolExecutorBuilder withTimeOut(long timeOut, @NonNull TimeUnit units) {
      this.timeOut = timeOut;
      this.units = units;
      return this;
    }

    public ThreadPoolExecutorBuilder withQueue(@NonNull final BlockingQueue<Runnable> queue) {
      this.queue = queue;
      return this;
    }

    public ThreadPoolExecutorBuilder atPriority(@NonNull final OptionalInt priority) {
      this.priority = priority;
      return this;
    }

    public ThreadPoolExecutorBuilder enableThreadPoolMetrics() {
      return enableThreadPoolMetrics(true);
    }

    public ThreadPoolExecutorBuilder enableThreadPoolMetrics(final boolean enable) {
      this.emitThreadPoolMetrics = enable;
      return this;
    }
  }

  private ThreadPoolExecutor createThreadPool(final int coreThreads, final int maxThreads,
      final long timeOut, final TimeUnit units, final String name,
      final BlockingQueue<Runnable> queue, final OptionalInt priority,
      final boolean emitThreadPoolMetrics) {
    LOG.trace(
        "Creating ThreadPoolExecutor for {} with {} core threads and {} max threads {} {} timeout",
        name, coreThreads, maxThreads, timeOut, units);
    var result = new ThreadPoolExecutor(coreThreads, maxThreads, timeOut, units, queue,
        new NamedThreadFactory(name, priority, handler)) {

      @Override
      public void execute(@NonNull Runnable arg0) {
        super.execute(TraceUtil.wrap(arg0));
      }

      @Override
      public boolean remove(Runnable task) {
        return super.remove(TraceUtil.wrap(task));
      }

      @Override
      @NonNull
      public <T> Future<T> submit(@NonNull Callable<T> task) {
        return super.submit(TraceUtil.wrap(task));
      }

      @Override
      @NonNull
      public <T> Future<T> submit(@NonNull Runnable task, T result) {
        return super.submit(TraceUtil.wrap(task), result);
      }

      @Override
      @NonNull
      public Future<?> submit(@NonNull Runnable task) {
        return super.submit(TraceUtil.wrap(task));
      }
    };
    if (timeOut > 0) {
      result.allowCoreThreadTimeOut(true);
    }
    if (emitThreadPoolMetrics) {
      addExecutorServiceMetrics(result, name);
    }
    return result;
  }

  public ScheduledThreadPoolExecutor
      createGeneralScheduledExecutorService(AccumuloConfiguration conf) {
    Property prop = conf.resolve(Property.GENERAL_THREADPOOL_SIZE,
        Property.GENERAL_SIMPLETIMER_THREADPOOL_SIZE);
    return (ScheduledThreadPoolExecutor) createExecutorService(conf, prop, true);
  }

  public ScheduledThreadPoolExecutor createScheduledExecutorService(int numThreads,
      final String name) {
    return createScheduledExecutorService(numThreads, name, false);
  }

  private ScheduledThreadPoolExecutor createScheduledExecutorService(int numThreads,
      final String name, boolean emitThreadPoolMetrics) {
    LOG.trace("Creating ScheduledThreadPoolExecutor for {} with {} threads", name, numThreads);
    var result =
        new ScheduledThreadPoolExecutor(numThreads, new NamedThreadFactory(name, handler)) {

          @Override
          public void execute(@NonNull Runnable command) {
            super.execute(TraceUtil.wrap(command));
          }

          @Override
          @NonNull
          public <V> ScheduledFuture<V> schedule(@NonNull Callable<V> callable, long delay,
              @NonNull TimeUnit unit) {
            return super.schedule(TraceUtil.wrap(callable), delay, unit);
          }

          @Override
          @NonNull
          public ScheduledFuture<?> schedule(@NonNull Runnable command, long delay,
              @NonNull TimeUnit unit) {
            return super.schedule(TraceUtil.wrap(command), delay, unit);
          }

          @Override
          @NonNull
          public ScheduledFuture<?> scheduleAtFixedRate(@NonNull Runnable command,
              long initialDelay, long period, @NonNull TimeUnit unit) {
            return super.scheduleAtFixedRate(TraceUtil.wrap(command), initialDelay, period, unit);
          }

          @Override
          @NonNull
          public ScheduledFuture<?> scheduleWithFixedDelay(@NonNull Runnable command,
              long initialDelay, long delay, @NonNull TimeUnit unit) {
            return super.scheduleWithFixedDelay(TraceUtil.wrap(command), initialDelay, delay, unit);
          }

          @Override
          @NonNull
          public <T> Future<T> submit(@NonNull Callable<T> task) {
            return super.submit(TraceUtil.wrap(task));
          }

          @Override
          @NonNull
          public <T> Future<T> submit(@NonNull Runnable task, T result) {
            return super.submit(TraceUtil.wrap(task), result);
          }

          @Override
          @NonNull
          public Future<?> submit(@NonNull Runnable task) {
            return super.submit(TraceUtil.wrap(task));
          }

          @Override
          public boolean remove(Runnable task) {
            return super.remove(TraceUtil.wrap(task));
          }

        };
    if (emitThreadPoolMetrics) {
      addExecutorServiceMetrics(result, name);
    }
    return result;
  }

  private static void addExecutorServiceMetrics(ExecutorService executor, String name) {
    new ExecutorServiceMetrics(executor, name, List.of()).bindTo(Metrics.globalRegistry);
  }
}
