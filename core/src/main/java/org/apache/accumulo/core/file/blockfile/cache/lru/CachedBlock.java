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

package org.apache.accumulo.core.file.blockfile.cache.lru;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.apache.accumulo.core.file.blockfile.cache.impl.ClassSize;
import org.apache.accumulo.core.file.blockfile.cache.impl.SizeConstants;
import org.apache.accumulo.core.spi.cache.CacheEntry.Weighable;

/**
 * Represents an entry in the configurable block cache.
 *
 * <p>
 * Makes the block memory-aware with {@link HeapSize} and Comparable to sort by access time for the
 * LRU. It also takes care of priority by either instantiating as in-memory or handling the
 * transition from single to multiple access.
 */
public class CachedBlock implements HeapSize, Comparable<CachedBlock> {

  public static final long PER_BLOCK_OVERHEAD =
      ClassSize.align(ClassSize.OBJECT + (3 * ClassSize.REFERENCE) + (2 * SizeConstants.SIZEOF_LONG)
          + ClassSize.STRING + ClassSize.BYTE_BUFFER + ClassSize.REFERENCE);

  public enum BlockPriority {
    SINGLE, // Accessed a single time (used for scan-resistance)
    MULTI, // Accessed multiple times
    MEMORY // Block from in-memory store
  }

  private final byte[] buffer;
  private final String blockName;
  private volatile long accessTime;
  private volatile long recordedSize;
  private BlockPriority priority;
  private Weighable index;

  public CachedBlock(String blockName, byte[] buf, long accessTime, boolean inMemory) {
    this.buffer = buf;
    this.blockName = blockName;
    this.accessTime = accessTime;
    this.priority = inMemory ? BlockPriority.MEMORY : BlockPriority.SINGLE;
  }

  public void access(long accessTime) {
    this.accessTime = accessTime;
    if (this.priority == BlockPriority.SINGLE) {
      this.priority = BlockPriority.MULTI;
    }
  }

  @Override
  public long heapSize() {
    validateEviction();
    return recordedSize;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(accessTime);
  }

  @Override
  public boolean equals(Object obj) {
    return this == obj || (obj instanceof CachedBlock && compareTo((CachedBlock) obj) == 0);
  }

  @Override
  public int compareTo(CachedBlock that) {
    return Long.compare(that.accessTime, this.accessTime);
  }

  public String getName() {
    return this.blockName;
  }

  public BlockPriority getPriority() {
    return this.priority;
  }

  public byte[] getBuffer() {
    return buffer;
  }

  @SuppressWarnings("unchecked")
  public synchronized <T extends Weighable> T getIndex(Supplier<T> supplier) {
    if (index == null && recordedSize >= 0) {
      index = supplier.get();
    }
    return (T) index;
  }

  private synchronized long calculateRecordSize() {
    long indexSize = (index == null) ? 0 : index.weight();
    return ClassSize.align(blockName.length()) + ClassSize.align(buffer.length) + PER_BLOCK_OVERHEAD
        + indexSize;
  }

  private synchronized long updateTotalSize(AtomicLong totalSize, long newSize) {
    long delta = newSize - recordedSize;
    recordedSize = newSize;
    return totalSize.addAndGet(delta);
  }

  public synchronized long tryRecordSize(AtomicLong totalSize) {
    return recordedSize < 0 ? -1 : updateTotalSize(totalSize, calculateRecordSize());
  }

  public synchronized long recordSize(AtomicLong totalSize) {
    validateEviction();
    return updateTotalSize(totalSize, calculateRecordSize());
  }

  public synchronized long evicted(AtomicLong totalSize) {
    validateEviction();
    long tmp = recordedSize;
    totalSize.addAndGet(-recordedSize);
    recordedSize = -1;
    index = null;
    return tmp;
  }

  private void validateEviction() {
    if (recordedSize < 0) {
      throw new IllegalStateException("Block was evicted: " + blockName);
    }
  }
}
