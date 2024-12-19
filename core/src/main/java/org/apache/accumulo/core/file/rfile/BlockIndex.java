
package org.apache.accumulo.core.file.rfile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.blockfile.cache.impl.ClassSize;
import org.apache.accumulo.core.file.blockfile.cache.impl.SizeConstants;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile.CachedBlockRead;
import org.apache.accumulo.core.file.rfile.MultiLevelIndex.IndexEntry;
import org.apache.accumulo.core.spi.cache.CacheEntry.Weighable;

public class BlockIndex implements Weighable {

  private BlockIndex() {}

  public static BlockIndex getIndex(CachedBlockRead cacheBlock, IndexEntry indexEntry)
      throws IOException {
    BlockIndex blockIndex = cacheBlock.getIndex(BlockIndex::new);
    if (blockIndex == null) {
      return null;
    }

    int accessCount = blockIndex.accessCount.incrementAndGet();

    if (accessCount >= 2 && isPowerOfTwo(accessCount)) {
      blockIndex.buildIndex(accessCount, cacheBlock, indexEntry);
      cacheBlock.indexWeightChanged();
    }

    return blockIndex.blockIndex != null ? blockIndex : null;
  }

  private static boolean isPowerOfTwo(int x) {
    return ((x > 0) && (x & (x - 1)) == 0);
  }

  private final AtomicInteger accessCount = new AtomicInteger(0);
  private volatile BlockIndexEntry[] blockIndex = null;

  public static class BlockIndexEntry implements Comparable<BlockIndexEntry> {
    private final Key prevKey;
    private int entriesLeft;
    private int pos;

    public BlockIndexEntry(int pos, int entriesLeft, Key prevKey) {
      this.pos = pos;
      this.entriesLeft = entriesLeft;
      this.prevKey = prevKey;
    }

    public BlockIndexEntry(Key key) {
      this.prevKey = key;
    }

    public int getEntriesLeft() {
      return entriesLeft;
    }

    @Override
    public int compareTo(BlockIndexEntry o) {
      return prevKey.compareTo(o.prevKey);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (!(o instanceof BlockIndexEntry))
        return false;
      BlockIndexEntry that = (BlockIndexEntry) o;
      return prevKey.equals(that.prevKey);
    }

    @Override
    public String toString() {
      return prevKey + " " + entriesLeft + " " + pos;
    }

    public Key getPrevKey() {
      return prevKey;
    }

    @Override
    public int hashCode() {
      return prevKey.hashCode();
    }

    int weight() {
      int keyWeight = ClassSize.align(prevKey.getSize()) + ClassSize.OBJECT
          + SizeConstants.SIZEOF_LONG + 4 * (ClassSize.ARRAY + ClassSize.REFERENCE);
      return 2 * SizeConstants.SIZEOF_INT + ClassSize.REFERENCE + ClassSize.OBJECT + keyWeight;
    }
  }

  public BlockIndexEntry seekBlock(Key startKey, CachedBlockRead cacheBlock) {
    BlockIndexEntry[] blockIndex = this.blockIndex;
    int pos = Arrays.binarySearch(blockIndex, new BlockIndexEntry(startKey));
    int index = determineIndex(pos, startKey, blockIndex);

    if (index == 0 && blockIndex[index].getPrevKey().equals(startKey)) {
      return null;
    }

    BlockIndexEntry bie = blockIndex[index];
    cacheBlock.seek(bie.pos);
    return bie;
  }

  private int determineIndex(int pos, Key startKey, BlockIndexEntry[] blockIndex) {
    if (pos < 0) {
      return (pos == -1) ? -1 : (pos * -1) - 2;
    } else {
      int index = adjustIndexForExactMatch(pos, startKey, blockIndex);
      return adjustIndexForDuplicateKeys(index, blockIndex);
    }
  }

  private int adjustIndexForExactMatch(int pos, Key startKey, BlockIndexEntry[] blockIndex) {
    int index = pos;
    while (index > 0 && blockIndex[index].getPrevKey().equals(startKey)) {
      index--;
    }
    return index;
  }

  private int adjustIndexForDuplicateKeys(int index, BlockIndexEntry[] blockIndex) {
    while (index - 1 > 0
        && blockIndex[index].getPrevKey().equals(blockIndex[index - 1].getPrevKey())) {
      index--;
    }
    return index;
  }

  private synchronized void buildIndex(int indexEntries, CachedBlockRead cacheBlock,
      IndexEntry indexEntry) throws IOException {
    cacheBlock.seek(0);

    RelativeKey rk = new RelativeKey();
    Value val = new Value();

    int interval = indexEntry.getNumEntries() / indexEntries;

    if (interval <= 32 || (blockIndex != null && blockIndex.length > indexEntries - 1)) {
      return;
    }

    ArrayList<BlockIndexEntry> index = new ArrayList<>(indexEntries - 1);
    int count = buildIndexEntries(interval, indexEntry, cacheBlock, rk, val, index);

    this.blockIndex = index.toArray(new BlockIndexEntry[0]);
    cacheBlock.seek(0);
  }

  private int buildIndexEntries(int interval, IndexEntry indexEntry, CachedBlockRead cacheBlock,
      RelativeKey rk, Value val, ArrayList<BlockIndexEntry> index) throws IOException {
    int count = 0;
    while (count < (indexEntry.getNumEntries() - interval + 1)) {
      int pos = cacheBlock.getPosition();
      Key myPrevKey = rk.getKey();
      rk.readFields(cacheBlock);
      val.readFields(cacheBlock);

      if (count > 0 && count % interval == 0) {
        index.add(new BlockIndexEntry(pos, indexEntry.getNumEntries() - count, myPrevKey));
      }
      count++;
    }
    return count;
  }

  BlockIndexEntry[] getIndexEntries() {
    return blockIndex;
  }

  @Override
  public synchronized int weight() {
    int weight = calculateWeight();
    weight +=
        ClassSize.ATOMIC_INTEGER + ClassSize.OBJECT + 2 * ClassSize.REFERENCE + ClassSize.ARRAY;
    return weight;
  }

  private int calculateWeight() {
    int weight = 0;
    if (blockIndex != null) {
      for (BlockIndexEntry blockIndexEntry : blockIndex) {
        weight += blockIndexEntry.weight();
      }
    }
    return weight;
  }
}
