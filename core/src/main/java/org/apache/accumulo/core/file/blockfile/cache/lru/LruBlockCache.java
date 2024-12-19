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

import static java.util.concurrent.TimeUnit.SECONDS;

import java.lang.ref.WeakReference;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import org.apache.accumulo.core.file.blockfile.cache.impl.ClassSize;
import org.apache.accumulo.core.file.blockfile.cache.impl.SizeConstants;
import org.apache.accumulo.core.spi.cache.BlockCache;
import org.apache.accumulo.core.spi.cache.CacheEntry;
import org.apache.accumulo.core.util.threads.ThreadPools;
import org.apache.accumulo.core.util.threads.Threads.AccumuloDaemonThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class LruBlockCache extends SynchronousLoadingBlockCache implements BlockCache, HeapSize {

  private static final Logger log = LoggerFactory.getLogger(LruBlockCache.class);
  static final int statThreadPeriod = 60;

  private final ConcurrentHashMap<String,CachedBlock> map;
  private final ReentrantLock evictionLock = new ReentrantLock(true);
  private volatile boolean evictionInProgress = false;
  private final EvictionThread evictionThread;
  private final ScheduledExecutorService scheduleThreadPool =
      ThreadPools.getServerThreadPools().createScheduledExecutorService(1, "LRUBlockCacheStats");

  private final AtomicLong size;
  private final AtomicLong elements;
  private final AtomicLong count;
  private final CacheStats stats;
  private final long overhead;
  private final LruBlockCacheConfiguration conf;

  @SuppressFBWarnings(value = "SC_START_IN_CTOR",
      justification = "bad practice to start threads in constructor; probably needs rewrite")
  public LruBlockCache(final LruBlockCacheConfiguration conf) {
    this.conf = conf;
    int mapInitialSize = calculateMapInitialSize();
    map = new ConcurrentHashMap<>(mapInitialSize, conf.getMapLoadFactor(),
        conf.getMapConcurrencyLevel());
    this.stats = new CacheStats();
    this.count = new AtomicLong(0);
    this.elements = new AtomicLong(0);
    this.overhead =
        calculateOverhead(conf.getMaxSize(), conf.getBlockSize(), conf.getMapConcurrencyLevel());
    this.size = new AtomicLong(this.overhead);
    this.evictionThread = initializeEvictionThread();
    scheduleStatisticsThread();
  }

  private int calculateMapInitialSize() {
    return (int) Math.ceil(1.2 * conf.getMaxSize() / conf.getBlockSize());
  }

  private EvictionThread initializeEvictionThread() {
    if (conf.isUseEvictionThread()) {
      EvictionThread thread = new EvictionThread(this);
      thread.start();
      waitForEvictionThread(thread);
      return thread;
    }
    return null;
  }

  private void waitForEvictionThread(EvictionThread thread) {
    while (!thread.running()) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  private void scheduleStatisticsThread() {
    ScheduledFuture<?> future = this.scheduleThreadPool.scheduleAtFixedRate(
        new StatisticsThread(this), statThreadPeriod, statThreadPeriod, SECONDS);
    ThreadPools.watchNonCriticalScheduledTask(future);
  }

  public long getOverhead() {
    return overhead;
  }

  private class LruCacheEntry implements CacheEntry {
    private final CachedBlock block;

    LruCacheEntry(CachedBlock block) {
      this.block = block;
    }

    @Override
    public byte[] getBuffer() {
      return block.getBuffer();
    }

    @Override
    public <T extends Weighable> T getIndex(Supplier<T> supplier) {
      return block.getIndex(supplier);
    }

    @Override
    public void indexWeightChanged() {
      long newSize = block.tryRecordSize(size);
      if (newSize >= 0 && newSize > acceptableSize() && !evictionInProgress) {
        runEviction();
      }
    }
  }

  private CacheEntry wrap(CachedBlock cb) {
    return cb == null ? null : new LruCacheEntry(cb);
  }

  public CacheEntry cacheBlock(String blockName, byte[] buf, boolean inMemory) {
    CachedBlock cb = map.get(blockName);
    if (cb != null) {
      handleDuplicateCacheBlock(cb);
    } else {
      handleNewCacheBlock(blockName, buf, inMemory);
    }
    return wrap(cb);
  }

  private void handleDuplicateCacheBlock(CachedBlock cb) {
    stats.duplicateReads();
    cb.access(count.incrementAndGet());
  }

  private void handleNewCacheBlock(String blockName, byte[] buf, boolean inMemory) {
    CachedBlock cb = new CachedBlock(blockName, buf, count.incrementAndGet(), inMemory);
    CachedBlock currCb = map.putIfAbsent(blockName, cb);
    if (currCb != null) {
      handleDuplicateCacheBlock(currCb);
      cb = currCb;
    } else {
      long newSize = cb.recordSize(size);
      elements.incrementAndGet();
      if (newSize > acceptableSize() && !evictionInProgress) {
        runEviction();
      }
    }
  }

  @Override
  public CacheEntry cacheBlock(String blockName, byte[] buf) {
    return cacheBlock(blockName, buf, false);
  }

  @Override
  public CacheEntry getBlock(String blockName) {
    CachedBlock cb = map.get(blockName);
    if (cb == null) {
      stats.miss();
      return null;
    }
    stats.hit();
    cb.access(count.incrementAndGet());
    return wrap(cb);
  }

  @Override
  protected CacheEntry getBlockNoStats(String blockName) {
    CachedBlock cb = map.get(blockName);
    if (cb != null) {
      cb.access(count.incrementAndGet());
    }
    return wrap(cb);
  }

  protected long evictBlock(CachedBlock block) {
    if (map.remove(block.getName()) != null) {
      elements.decrementAndGet();
      stats.evicted();
      return block.evicted(size);
    }
    return 0;
  }

  private void runEviction() {
    if (evictionThread == null) {
      evict();
    } else {
      evictionThread.evict();
    }
  }

  void evict() {
    if (!evictionLock.tryLock()) {
      return;
    }

    try {
      evictionInProgress = true;
      long bytesToFree = size.get() - minSize();
      log.trace("Block cache LRU eviction started.  Attempting to free {} bytes", bytesToFree);

      if (bytesToFree <= 0) {
        return;
      }

      PriorityQueue<BlockBucket> bucketQueue = initializeBuckets(bytesToFree);
      long bytesFreed = freeBuckets(bucketQueue, bytesToFree);

      logEvictionCompletion(bytesFreed, bucketQueue);

    } finally {
      stats.evict();
      evictionInProgress = false;
      evictionLock.unlock();
    }
  }

  private PriorityQueue<BlockBucket> initializeBuckets(long bytesToFree) {
    BlockBucket bucketSingle = new BlockBucket(bytesToFree, conf.getBlockSize(), singleSize());
    BlockBucket bucketMulti = new BlockBucket(bytesToFree, conf.getBlockSize(), multiSize());
    BlockBucket bucketMemory = new BlockBucket(bytesToFree, conf.getBlockSize(), memorySize());

    for (CachedBlock cachedBlock : map.values()) {
      addBlockToBucket(cachedBlock, bucketSingle, bucketMulti, bucketMemory);
    }

    PriorityQueue<BlockBucket> bucketQueue = new PriorityQueue<>(3);
    bucketQueue.add(bucketSingle);
    bucketQueue.add(bucketMulti);
    bucketQueue.add(bucketMemory);
    return bucketQueue;
  }

  private void addBlockToBucket(CachedBlock cachedBlock, BlockBucket single, BlockBucket multi,
      BlockBucket memory) {
    switch (cachedBlock.getPriority()) {
      case SINGLE:
        single.add(cachedBlock);
        break;
      case MULTI:
        multi.add(cachedBlock);
        break;
      case MEMORY:
        memory.add(cachedBlock);
        break;
    }
  }

  private long freeBuckets(PriorityQueue<BlockBucket> bucketQueue, long bytesToFree) {
    int remainingBuckets = 3;
    long bytesFreed = 0;
    BlockBucket bucket;
    while ((bucket = bucketQueue.poll()) != null) {
      long overflow = bucket.overflow();
      if (overflow > 0) {
        long bucketBytesToFree = Math.min(overflow,
            (long) Math.ceil((bytesToFree - bytesFreed) / (double) remainingBuckets));
        bytesFreed += bucket.free(bucketBytesToFree);
      }
      remainingBuckets--;
    }
    return bytesFreed;
  }

  private void logEvictionCompletion(long bytesFreed, PriorityQueue<BlockBucket> buckets) {
    float singleMB = ((float) buckets.peek().totalSize()) / ((float) (1024 * 1024));
    float multiMB = ((float) buckets.peek().totalSize()) / ((float) (1024 * 1024));
    float memoryMB = ((float) buckets.peek().totalSize()) / ((float) (1024 * 1024));

    log.trace(
        "Block cache LRU eviction completed. Freed {} bytes. Priority Sizes:"
            + " Single={}MB ({}), Multi={}MB ({}), Memory={}MB ({})",
        bytesFreed, singleMB, buckets.peek().totalSize(), multiMB, buckets.peek().totalSize(),
        memoryMB, buckets.peek().totalSize());
  }

  private class BlockBucket implements Comparable<BlockBucket> {
    private final CachedBlockQueue queue;
    private long totalSize;
    private final long bucketSize;

    public BlockBucket(long bytesToFree, long blockSize, long bucketSize) {
      this.bucketSize = bucketSize;
      queue = new CachedBlockQueue(bytesToFree, blockSize);
      totalSize = 0;
    }

    public void add(CachedBlock block) {
      totalSize += block.heapSize();
      queue.add(block);
    }

    public long free(long toFree) {
      CachedBlock[] blocks = queue.get();
      long freedBytes = 0;
      for (CachedBlock block : blocks) {
        freedBytes += evictBlock(block);
        if (freedBytes >= toFree) {
          return freedBytes;
        }
      }
      return freedBytes;
    }

    public long overflow() {
      return totalSize - bucketSize;
    }

    public long totalSize() {
      return totalSize;
    }

    @Override
    public int compareTo(BlockBucket that) {
      return Long.compare(this.overflow(), that.overflow());
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(overflow());
    }

    @Override
    public boolean equals(Object that) {
      if (that instanceof BlockBucket) {
        return compareTo((BlockBucket) that) == 0;
      }
      return false;
    }
  }

  @Override
  public long getMaxHeapSize() {
    return getMaxSize();
  }

  @Override
  public long getMaxSize() {
    return this.conf.getMaxSize();
  }

  @Override
  public int getMaxEntrySize() {
    return (int) Math.min(Integer.MAX_VALUE, getMaxSize());
  }

  public long getCurrentSize() {
    return this.size.get();
  }

  public long getFreeSize() {
    return getMaxSize() - getCurrentSize();
  }

  public long size() {
    return this.elements.get();
  }

  public long getEvictionCount() {
    return this.stats.getEvictionCount();
  }

  public long getEvictedCount() {
    return this.stats.getEvictedCount();
  }

  private static class EvictionThread extends AccumuloDaemonThread {
    private final WeakReference<LruBlockCache> cache;
    private boolean running = false;

    public EvictionThread(LruBlockCache cache) {
      super("LruBlockCache.EvictionThread");
      this.cache = new WeakReference<>(cache);
    }

    public synchronized boolean running() {
      return running;
    }

    @Override
    public void run() {
      while (true) {
        waitForEviction();
        LruBlockCache cache = this.cache.get();
        if (cache == null) {
          break;
        }
        cache.evict();
      }
    }

    private synchronized void waitForEviction() {
      running = true;
      try {
        this.wait();
      } catch (InterruptedException e) {
        // empty
      }
    }

    public void evict() {
      synchronized (this) {
        this.notify();
      }
    }
  }

  private static class StatisticsThread extends AccumuloDaemonThread {
    LruBlockCache lru;

    public StatisticsThread(LruBlockCache lru) {
      super("LruBlockCache.StatisticsThread");
      this.lru = lru;
    }

    @Override
    public void run() {
      lru.logStats();
    }
  }

  public void logStats() {
    long totalSize = heapSize();
    long freeSize = this.conf.getMaxSize() - totalSize;
    float sizeMB = ((float) totalSize) / ((float) (1024 * 1024));
    float freeMB = ((float) freeSize) / ((float) (1024 * 1024));
    float maxMB = ((float) this.conf.getMaxSize()) / ((float) (1024 * 1024));
    log.debug(
        "Cache Stats: {} Sizes: Total={}MB ({}), Free={}MB ({}), Max={}MB"
            + " ({}), Counts: Blocks={}, Access={}, Hit={}, Miss={}, Evictions={},"
            + " Evicted={},Ratios: Hit Ratio={}%, Miss Ratio={}%, Evicted/Run={},"
            + " Duplicate Reads={}",
        conf.getCacheType(), sizeMB, totalSize, freeMB, freeSize, maxMB, this.conf.getMaxSize(),
        size(), stats.requestCount(), stats.hitCount(), stats.getMissCount(),
        stats.getEvictionCount(), stats.getEvictedCount(), stats.getHitRatio() * 100,
        stats.getMissRatio() * 100, stats.evictedPerEviction(), stats.getDuplicateReads());
  }

  @Override
  public CacheStats getStats() {
    return this.stats;
  }

  public static class CacheStats implements BlockCache.Stats {
    private final AtomicLong accessCount = new AtomicLong(0);
    private final AtomicLong hitCount = new AtomicLong(0);
    private final AtomicLong missCount = new AtomicLong(0);
    private final AtomicLong evictionCount = new AtomicLong(0);
    private final AtomicLong evictedCount = new AtomicLong(0);
    private final AtomicLong duplicateReads = new AtomicLong(0);

    public void miss() {
      missCount.incrementAndGet();
      accessCount.incrementAndGet();
    }

    public void hit() {
      hitCount.incrementAndGet();
      accessCount.incrementAndGet();
    }

    public void evict() {
      evictionCount.incrementAndGet();
    }

    public void duplicateReads() {
      duplicateReads.incrementAndGet();
    }

    public void evicted() {
      evictedCount.incrementAndGet();
    }

    @Override
    public long requestCount() {
      return accessCount.get();
    }

    public long getMissCount() {
      return missCount.get();
    }

    @Override
    public long hitCount() {
      return hitCount.get();
    }

    public long getEvictionCount() {
      return evictionCount.get();
    }

    public long getDuplicateReads() {
      return duplicateReads.get();
    }

    public long getEvictedCount() {
      return evictedCount.get();
    }

    public double getHitRatio() {
      return ((float) hitCount() / (float) requestCount());
    }

    public double getMissRatio() {
      return ((float) getMissCount() / (float) requestCount());
    }

    public double evictedPerEviction() {
      return (float) getEvictedCount() / (float) getEvictionCount();
    }
  }

  public static final long CACHE_FIXED_OVERHEAD =
      ClassSize.align((3 * SizeConstants.SIZEOF_LONG) + (8 * ClassSize.REFERENCE)
          + (5 * SizeConstants.SIZEOF_FLOAT) + SizeConstants.SIZEOF_BOOLEAN + ClassSize.OBJECT);

  @Override
  public long heapSize() {
    return getCurrentSize();
  }

  public static long calculateOverhead(long maxSize, long blockSize, int concurrency) {
    long entryPart = Math.round(maxSize * 1.2 / blockSize) * ClassSize.CONCURRENT_HASHMAP_ENTRY;
    long segmentPart = (long) concurrency * ClassSize.CONCURRENT_HASHMAP_SEGMENT;
    return CACHE_FIXED_OVERHEAD + ClassSize.CONCURRENT_HASHMAP + entryPart + segmentPart;
  }

  private long acceptableSize() {
    return (long) Math.floor(this.conf.getMaxSize() * this.conf.getAcceptableFactor());
  }

  private long minSize() {
    return (long) Math.floor(this.conf.getMaxSize() * this.conf.getMinFactor());
  }

  private long singleSize() {
    return (long) Math
        .floor(this.conf.getMaxSize() * this.conf.getSingleFactor() * this.conf.getMinFactor());
  }

  private long multiSize() {
    return (long) Math
        .floor(this.conf.getMaxSize() * this.conf.getMultiFactor() * this.conf.getMinFactor());
  }

  private long memorySize() {
    return (long) Math
        .floor(this.conf.getMaxSize() * this.conf.getMemoryFactor() * this.conf.getMinFactor());
  }

  public void shutdown() {
    this.scheduleThreadPool.shutdown();
  }
}
