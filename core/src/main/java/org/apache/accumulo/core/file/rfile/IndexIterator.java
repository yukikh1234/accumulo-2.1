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

package org.apache.accumulo.core.file.rfile;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.rfile.MultiLevelIndex.IndexEntry;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

class IndexIterator implements SortedKeyValueIterator<Key,Value> {

  private Key key;
  private final Iterator<IndexEntry> indexIter;

  IndexIterator(Iterator<IndexEntry> indexIter) {
    this.indexIter = indexIter;
    advance();
  }

  private void advance() {
    if (indexIter.hasNext()) {
      key = indexIter.next().getKey();
    } else {
      key = null;
    }
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    // Purpose: To create a meaningful copy of the iterator in the given environment.
    // Since the current implementation is not supported, throwing an exception.
    throw new UnsupportedOperationException("DeepCopy is not supported.");
  }

  @Override
  public Key getTopKey() {
    return key;
  }

  @Override
  public Value getTopValue() {
    // Purpose: To return the top value associated with the current top key.
    // Since the current implementation does not support values, throwing an exception.
    throw new UnsupportedOperationException("GetTopValue is not supported.");
  }

  @Override
  public boolean hasTop() {
    return key != null;
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    // Purpose: To initialize the iterator with a source and configuration options.
    // Custom implementation required if needed in the future.
    throw new UnsupportedOperationException("Init is not supported.");
  }

  @Override
  public void next() throws IOException {
    advance();
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    // Purpose: To position the iterator at the first key-value pair that matches the given range.
    // Custom implementation required if needed in the future.
    throw new UnsupportedOperationException("Seek is not supported.");
  }

}
