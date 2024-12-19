
package org.apache.accumulo.core.file.blockfile.cache.lru;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.accumulo.core.spi.cache.BlockCache;
import org.apache.accumulo.core.spi.cache.CacheEntry;

/**
 * This class implements loading in such a way that load operations for the same block will not run
 * concurrently.
 */
public abstract class SynchronousLoadingBlockCache implements BlockCache {

  private final Lock[] loadLocks;

  /**
   * @param numLocks this controls how many load operations can run concurrently
   */
  SynchronousLoadingBlockCache(int numLocks) {
    loadLocks = new Lock[numLocks];
    for (int i = 0; i < loadLocks.length; i++) {
      loadLocks[i] = new ReentrantLock(true);
    }
  }

  public SynchronousLoadingBlockCache() {
    this(5003);
  }

  private Map<String,byte[]> resolveDependencies(Map<String,Loader> loaderDeps) {
    if (loaderDeps.isEmpty()) {
      return Collections.emptyMap();
    } else if (loaderDeps.size() == 1) {
      return resolveSingleDependency(loaderDeps);
    } else {
      return resolveMultipleDependencies(loaderDeps);
    }
  }

  private Map<String,byte[]> resolveSingleDependency(Map<String,Loader> loaderDeps) {
    Entry<String,Loader> entry = loaderDeps.entrySet().iterator().next();
    CacheEntry dce = getBlock(entry.getKey(), entry.getValue());
    if (dce == null) {
      return null;
    } else {
      return Collections.singletonMap(entry.getKey(), dce.getBuffer());
    }
  }

  private Map<String,byte[]> resolveMultipleDependencies(Map<String,Loader> loaderDeps) {
    Map<String,byte[]> depData = new HashMap<>();
    for (Entry<String,Loader> entry : loaderDeps.entrySet()) {
      CacheEntry dce = getBlock(entry.getKey(), entry.getValue());
      if (dce == null) {
        return null;
      }
      depData.put(entry.getKey(), dce.getBuffer());
    }
    return depData;
  }

  /**
   * Get the maximum size of an individual cache entry.
   */
  protected abstract int getMaxEntrySize();

  /**
   * Get a block from the cache without changing any stats the cache is keeping.
   */
  protected abstract CacheEntry getBlockNoStats(String blockName);

  @Override
  public CacheEntry getBlock(String blockName, Loader loader) {
    CacheEntry ce = getBlock(blockName);
    if (ce != null) {
      return ce;
    }

    Map<String,byte[]> depData = resolveDependencies(loader.getDependencies());
    if (depData == null) {
      return null;
    }

    int lockIndex = (blockName.hashCode() & 0x7fffffff) % loadLocks.length;
    Lock loadLock = loadLocks[lockIndex];

    loadLock.lock();
    try {
      return performLoading(blockName, loader, depData);
    } finally {
      loadLock.unlock();
    }
  }

  private CacheEntry performLoading(String blockName, Loader loader, Map<String,byte[]> depData) {
    CacheEntry ce = getBlockNoStats(blockName);
    if (ce != null) {
      return ce;
    }

    byte[] data = loader.load(getMaxEntrySize(), depData);
    if (data == null) {
      return null;
    }

    return cacheBlock(blockName, data);
  }
}
