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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.YieldCallback;

public class SourceSwitchingIterator implements InterruptibleIterator {

  public interface DataSource {
    boolean isCurrent();

    DataSource getNewDataSource();

    DataSource getDeepCopyDataSource(IteratorEnvironment env);

    SortedKeyValueIterator<Key,Value> iterator() throws IOException;

    void setInterruptFlag(AtomicBoolean flag);

    default void close(boolean sawErrors) {}
  }

  private DataSource source;
  private SortedKeyValueIterator<Key,Value> iter;
  private Optional<YieldCallback<Key>> yield = Optional.empty();
  private Key key;
  private Value val;
  private Range range;
  private boolean inclusive;
  private Collection<ByteSequence> columnFamilies;
  private final boolean onlySwitchAfterRow;
  private final List<SourceSwitchingIterator> copies;

  private SourceSwitchingIterator(DataSource source, boolean onlySwitchAfterRow,
      List<SourceSwitchingIterator> copies) {
    this.source = source;
    this.onlySwitchAfterRow = onlySwitchAfterRow;
    this.copies = copies;
    copies.add(this);
  }

  public SourceSwitchingIterator(DataSource source, boolean onlySwitchAfterRow) {
    this(source, onlySwitchAfterRow, new ArrayList<>());
  }

  public SourceSwitchingIterator(DataSource source) {
    this(source, false);
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    synchronized (copies) {
      return new SourceSwitchingIterator(source.getDeepCopyDataSource(env), onlySwitchAfterRow,
          copies);
    }
  }

  @Override
  public Key getTopKey() {
    return key;
  }

  @Override
  public Value getTopValue() {
    return val;
  }

  @Override
  public boolean hasTop() {
    return key != null;
  }

  @Override
  public void enableYielding(YieldCallback<Key> yield) {
    this.yield = Optional.of(yield);
    if (!onlySwitchAfterRow && iter != null) {
      iter.enableYielding(yield);
    }
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void next() throws IOException {
    synchronized (copies) {
      readNext(false);
    }
  }

  private void readNext(boolean initialSeek) throws IOException {
    boolean yielded = yield.isPresent() && yield.orElseThrow().hasYielded();
    boolean seekNeeded = yielded || (!onlySwitchAfterRow && switchSource()) || initialSeek;

    if (seekNeeded) {
      doSeek(yielded, initialSeek);
    } else {
      iter.next();
      handleRowSwitch();
    }

    updateTopKeyValue();
  }

  private void doSeek(boolean yielded, boolean initialSeek) throws IOException {
    if (initialSeek) {
      iter.seek(range, columnFamilies, inclusive);
    } else if (yielded) {
      Key yieldPosition = yield.orElseThrow().getPositionAndReset();
      validateYieldPosition(yieldPosition);
      iter.seek(new Range(yieldPosition, false, range.getEndKey(), range.isEndKeyInclusive()),
          columnFamilies, inclusive);
    } else {
      iter.seek(new Range(key, false, range.getEndKey(), range.isEndKeyInclusive()), columnFamilies,
          inclusive);
    }
  }

  private void validateYieldPosition(Key yieldPosition) throws IOException {
    if (!range.contains(yieldPosition)) {
      throw new IOException(
          "Yielded to a position outside of its range: " + yieldPosition + " not in " + range);
    }
  }

  private void handleRowSwitch() throws IOException {
    if (onlySwitchAfterRow && iter.hasTop() && !source.isCurrent()
        && !key.getRowData().equals(iter.getTopKey().getRowData())) {
      switchSource();
      iter.seek(new Range(key.followingKey(PartialKey.ROW), true, range.getEndKey(),
          range.isEndKeyInclusive()), columnFamilies, inclusive);
    }
  }

  private void updateTopKeyValue() throws IOException {
    if (iter.hasTop()) {
      if (yield.isPresent() && yield.orElseThrow().hasYielded()) {
        throw new IOException("Coding error: hasTop returned true but has yielded at "
            + yield.orElseThrow().getPositionAndReset());
      }
      key = iter.getTopKey();
      val = iter.getTopValue();
    } else {
      key = null;
      val = null;
    }
  }

  private boolean switchSource() throws IOException {
    if (!source.isCurrent()) {
      source = source.getNewDataSource();
      iter = source.iterator();
      if (!onlySwitchAfterRow && yield.isPresent()) {
        iter.enableYielding(yield.orElseThrow());
      }
      return true;
    }
    return false;
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    synchronized (copies) {
      this.range = range;
      this.inclusive = inclusive;
      this.columnFamilies = columnFamilies;

      if (iter == null) {
        iter = source.iterator();
        if (!onlySwitchAfterRow && yield.isPresent()) {
          iter.enableYielding(yield.orElseThrow());
        }
      }

      readNext(true);
    }
  }

  private void _switchNow() throws IOException {
    if (onlySwitchAfterRow) {
      throw new IllegalStateException("Can only switch on row boundaries");
    }

    if (switchSource()) {
      if (key != null) {
        iter.seek(new Range(key, true, range.getEndKey(), range.isEndKeyInclusive()),
            columnFamilies, inclusive);
      }
    }
  }

  public void switchNow() throws IOException {
    synchronized (copies) {
      for (SourceSwitchingIterator ssi : copies) {
        ssi._switchNow();
      }
    }
  }

  @Override
  public void setInterruptFlag(AtomicBoolean flag) {
    synchronized (copies) {
      if (copies.size() != 1) {
        throw new IllegalStateException(
            "setInterruptFlag() called after deep copies made " + copies.size());
      }
      if (iter != null) {
        ((InterruptibleIterator) iter).setInterruptFlag(flag);
      }
      source.setInterruptFlag(flag);
    }
  }
}
