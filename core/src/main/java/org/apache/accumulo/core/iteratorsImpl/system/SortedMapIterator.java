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

package org.apache.accumulo.core.iteratorsImpl.system;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.client.SampleNotPresentException;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

public class SortedMapIterator implements InterruptibleIterator {
  private Iterator<Entry<Key,Value>> iter;
  private Entry<Key,Value> entry;
  private final SortedMap<Key,Value> map;
  private Range range;
  private AtomicBoolean interruptFlag;
  private int interruptCheckCount = 0;

  @Override
  public SortedMapIterator deepCopy(IteratorEnvironment env) {
    if (env != null && env.isSamplingEnabled()) {
      throw new SampleNotPresentException();
    }
    return new SortedMapIterator(map, interruptFlag);
  }

  private SortedMapIterator(SortedMap<Key,Value> map, AtomicBoolean interruptFlag) {
    this.map = map;
    this.interruptFlag = interruptFlag;
    reset();
  }

  public SortedMapIterator(SortedMap<Key,Value> map) {
    this(map, null);
  }

  private void reset() {
    this.iter = null;
    this.range = new Range();
    this.entry = null;
  }

  @Override
  public Key getTopKey() {
    return entry.getKey();
  }

  @Override
  public Value getTopValue() {
    return entry.getValue();
  }

  @Override
  public boolean hasTop() {
    return entry != null;
  }

  @Override
  public void next() throws IOException {
    validateCurrentEntry();
    checkForInterrupt();

    if (iter.hasNext()) {
      updateEntry();
    } else {
      entry = null;
    }
  }

  private void validateCurrentEntry() {
    if (entry == null) {
      throw new IllegalStateException();
    }
  }

  private void checkForInterrupt() {
    if (interruptFlag != null && interruptCheckCount++ % 100 == 0 && interruptFlag.get()) {
      throw new IterationInterruptedException();
    }
  }

  private void updateEntry() {
    entry = iter.next();
    if (range.afterEndKey(entry.getKey())) {
      entry = null;
    }
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    if (interruptFlag != null && interruptFlag.get()) {
      throw new IterationInterruptedException();
    }

    this.range = range;
    initiateIterator(range.getStartKey());

    while (hasTop() && range.beforeStartKey(getTopKey())) {
      next();
    }
  }

  private void initiateIterator(Key startKey) {
    Key key = startKey != null ? startKey : new Key();
    iter = map.tailMap(key).entrySet().iterator();
    if (iter.hasNext()) {
      updateEntry();
    } else {
      entry = null;
    }
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setInterruptFlag(AtomicBoolean flag) {
    this.interruptFlag = flag;
  }
}
