
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
package org.apache.accumulo.core.util;

import java.lang.ref.WeakReference;
import java.util.WeakHashMap;

/**
 * A utility that mimics String.intern() for any immutable object type (including String).
 */
public class Interner<T> {
  private final WeakHashMap<T,WeakReference<T>> internTable = new WeakHashMap<>();

  public synchronized T intern(T item) {
    T existingItem = getExistingItem(item);
    if (existingItem != null) {
      return existingItem;
    }
    internTable.put(item, new WeakReference<>(item));
    return item;
  }

  private T getExistingItem(T item) {
    WeakReference<T> ref = internTable.get(item);
    if (ref != null) {
      return ref.get();
    }
    return null;
  }

  // for testing
  synchronized int size() {
    return internTable.size();
  }
}
