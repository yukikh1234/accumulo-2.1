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

package org.apache.accumulo.core.util;

import java.util.HashMap;
import java.util.Set;
import java.util.stream.LongStream;

/**
 * A Map counter for counting with longs or integers. Not thread safe.
 */
public class MapCounter<KT> {

  static class MutableLong {
    long l = 0L;
  }

  private final HashMap<KT,MutableLong> map;

  public MapCounter() {
    map = new HashMap<>();
  }

  /**
   * Increments the value associated with the specified key by the given amount.
   *
   * @param key the key whose associated value is to be incremented
   * @param l the value to be added to the key's current value
   * @return the new value associated with the key after increment
   */
  public long increment(KT key, long l) {
    if (key == null || l == 0) {
      throw new IllegalArgumentException("Invalid key or increment value.");
    }

    MutableLong ml = map.computeIfAbsent(key, k -> new MutableLong());
    ml.l += l;

    if (ml.l == 0) {
      map.remove(key);
    }

    return ml.l;
  }

  /**
   * Decrements the value associated with the specified key by the given amount.
   *
   * @param key the key whose associated value is to be decremented
   * @param l the value to be subtracted from the key's current value
   * @return the new value associated with the key after decrement
   */
  public long decrement(KT key, long l) {
    return increment(key, -l);
  }

  /**
   * Retrieves the value associated with the specified key.
   *
   * @param key the key whose associated value is to be returned
   * @return the value associated with the key, or 0 if the key is not found
   */
  public long get(KT key) {
    MutableLong ml = map.get(key);
    return (ml == null) ? 0 : ml.l;
  }

  /**
   * Retrieves the value associated with the specified key as an integer.
   *
   * @param key the key whose associated value is to be returned as an integer
   * @return the value associated with the key as an integer, or throws an exception if conversion
   *         fails
   */
  public int getInt(KT key) {
    try {
      return Math.toIntExact(get(key));
    } catch (ArithmeticException e) {
      throw new IllegalStateException("Value cannot be converted to int", e);
    }
  }

  /**
   * Returns a set of keys contained in this map.
   *
   * @return a set of keys
   */
  public Set<KT> keySet() {
    return map.keySet();
  }

  /**
   * Returns a stream of values contained in this map.
   *
   * @return a stream of long values
   */
  public LongStream valueStream() {
    return map.values().stream().mapToLong(mutLong -> mutLong.l);
  }

  /**
   * Returns the maximum value contained in this map.
   *
   * @return the maximum value, or 0 if the map is empty
   */
  public long max() {
    return valueStream().max().orElse(0);
  }

  /**
   * Returns the number of key-value mappings in this map.
   *
   * @return the number of key-value mappings
   */
  public int size() {
    return map.size();
  }
}
