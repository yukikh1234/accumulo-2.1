
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

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.blockfile.impl.CacheProvider;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class MapFileIterator implements FileSKVIterator {

  private static final String MSG = "Map files are not supported";

  public MapFileIterator(FileSystem fs, String dir, Configuration conf) {
    unsupportedOperation();
  }

  @Override
  public void setInterruptFlag(AtomicBoolean flag) {
    unsupportedOperation();
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) {
    unsupportedOperation();
  }

  @Override
  public boolean hasTop() {
    return unsupportedOperation();
  }

  @Override
  public void next() {
    unsupportedOperation();
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) {
    unsupportedOperation();
  }

  @Override
  public Key getTopKey() {
    return unsupportedOperation();
  }

  @Override
  public Value getTopValue() {
    return unsupportedOperation();
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    return unsupportedOperation();
  }

  @Override
  public Key getFirstKey() {
    return unsupportedOperation();
  }

  @Override
  public Key getLastKey() {
    return unsupportedOperation();
  }

  @Override
  public DataInputStream getMetaStore(String name) {
    return unsupportedOperation();
  }

  @Override
  public long estimateOverlappingEntries(KeyExtent extent) throws IOException {
    return unsupportedOperation();
  }

  @Override
  public void closeDeepCopies() {
    unsupportedOperation();
  }

  @Override
  public void close() {
    unsupportedOperation();
  }

  @Override
  public FileSKVIterator getSample(SamplerConfigurationImpl sampleConfig) {
    return unsupportedOperation();
  }

  @Override
  public void setCacheProvider(CacheProvider cacheProvider) {
    unsupportedOperation();
  }

  private <T> T unsupportedOperation() {
    throw new UnsupportedOperationException(MSG);
  }
}
