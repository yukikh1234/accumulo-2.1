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
package org.apache.accumulo.core.iteratorsImpl.system;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

public class TimeSettingIterator implements InterruptibleIterator {

  private final SortedKeyValueIterator<Key,Value> source;
  private final long time;
  private Range range;

  public TimeSettingIterator(SortedKeyValueIterator<Key,Value> source, long time) {
    this.source = source;
    this.time = time;
  }

  @Override
  public Key getTopKey() {
    Key key = source.getTopKey();
    key.setTimestamp(time);
    return key;
  }

  @Override
  public void setInterruptFlag(AtomicBoolean flag) {
    ((InterruptibleIterator) source).setInterruptFlag(flag);
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    return new TimeSettingIterator(source.deepCopy(env), time);
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    if (source == null) {
      throw new IllegalArgumentException("Source iterator must not be null");
    }
    this.range = new Range();
  }

  @Override
  public boolean hasTop() {
    return source.hasTop() && !range.afterEndKey(getTopKey());
  }

  @Override
  public void next() throws IOException {
    source.next();
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    this.range = range;
    seekAndAdjustRange(columnFamilies, inclusive);
  }

  private void seekAndAdjustRange(Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    Range seekRange = IteratorUtil.maximizeStartKeyTimeStamp(range);
    seekRange = IteratorUtil.minimizeEndKeyTimeStamp(seekRange);
    source.seek(seekRange, columnFamilies, inclusive);
    adjustToValidTop();
  }

  private void adjustToValidTop() throws IOException {
    while (hasTop() && range.beforeStartKey(getTopKey())) {
      next();
    }
  }

  @Override
  public Value getTopValue() {
    return source.getTopValue();
  }
}
