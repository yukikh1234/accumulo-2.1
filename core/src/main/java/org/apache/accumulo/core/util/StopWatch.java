
package org.apache.accumulo.core.util;

import java.util.EnumMap;

public class StopWatch<K extends Enum<K>> {
  private final EnumMap<K,Long> startTime;
  private final EnumMap<K,Long> totalTime;

  public StopWatch(Class<K> k) {
    startTime = new EnumMap<>(k);
    totalTime = new EnumMap<>(k);
  }

  /**
   * Starts the timer for the specified key. Throws an exception if the timer is already started.
   *
   * @param timer the key for which the timer should be started
   * @throws IllegalStateException if the timer is already started
   */
  public synchronized void start(K timer) {
    if (startTime.containsKey(timer)) {
      throw new IllegalStateException(timer + " already started");
    }
    startTime.put(timer, System.currentTimeMillis());
  }

  /**
   * Stops the timer for the specified key and updates the total time. Throws an exception if the
   * timer was not started.
   *
   * @param timer the key for which the timer should be stopped
   * @throws IllegalStateException if the timer was not started
   */
  public synchronized void stop(K timer) {
    Long start = startTime.get(timer);
    if (start == null) {
      throw new IllegalStateException(timer + " not started");
    }

    long elapsedTime = System.currentTimeMillis() - start;
    totalTime.put(timer, totalTime.getOrDefault(timer, 0L) + elapsedTime);
    startTime.remove(timer);
  }

  /**
   * Retrieves the total time in milliseconds for the specified key.
   *
   * @param timer the key for which the total time should be retrieved
   * @return the total time in milliseconds
   */
  public synchronized long get(K timer) {
    return totalTime.getOrDefault(timer, 0L);
  }

  /**
   * Retrieves the total time in seconds for the specified key.
   *
   * @param timer the key for which the total time should be retrieved
   * @return the total time in seconds
   */
  public synchronized double getSecs(K timer) {
    return get(timer) / 1000.0;
  }
}
