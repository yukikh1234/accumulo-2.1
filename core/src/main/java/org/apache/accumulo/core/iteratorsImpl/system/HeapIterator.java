
package org.apache.accumulo.core.iteratorsImpl.system;

import java.io.IOException;
import java.util.PriorityQueue;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

/**
 * Constructs a {@link PriorityQueue} of multiple SortedKeyValueIterators. Provides a simple way to
 * interact with multiple SortedKeyValueIterators in sorted order.
 */
public abstract class HeapIterator implements SortedKeyValueIterator<Key,Value> {
  private PriorityQueue<SortedKeyValueIterator<Key,Value>> heap;
  private SortedKeyValueIterator<Key,Value> topIdx = null;
  private Key nextKey;

  protected HeapIterator() {
    heap = null;
  }

  protected HeapIterator(int maxSize) {
    createHeap(maxSize);
  }

  protected void createHeap(int maxSize) {
    if (heap != null) {
      throw new IllegalStateException("heap already exist");
    }
    heap = new PriorityQueue<>(Math.max(maxSize, 1),
        (si1, si2) -> si1.getTopKey().compareTo(si2.getTopKey()));
  }

  @Override
  public final Key getTopKey() {
    return topIdx.getTopKey();
  }

  @Override
  public final Value getTopValue() {
    return topIdx.getTopValue();
  }

  @Override
  public final boolean hasTop() {
    return topIdx != null;
  }

  @Override
  public final void next() throws IOException {
    if (topIdx == null) {
      throw new IllegalStateException("Called next() when there is no top");
    }
    topIdx.next();
    if (topIdx.hasTop()) {
      handleNextWithTop();
    } else {
      handleNextWithoutTop();
    }
  }

  private void handleNextWithTop() {
    if (nextKey != null && nextKey.compareTo(topIdx.getTopKey()) < 0) {
      SortedKeyValueIterator<Key,Value> nextTopIdx = heap.remove();
      heap.add(topIdx);
      topIdx = nextTopIdx;
      nextKey = heap.peek().getTopKey();
    }
  }

  private void handleNextWithoutTop() {
    if (nextKey == null) {
      topIdx = null;
    } else {
      pullReferencesFromHeap();
    }
  }

  private void pullReferencesFromHeap() {
    topIdx = heap.remove();
    nextKey = heap.isEmpty() ? null : heap.peek().getTopKey();
  }

  protected final void clear() {
    heap.clear();
    topIdx = null;
    nextKey = null;
  }

  protected final void addSource(SortedKeyValueIterator<Key,Value> source) {
    if (source.hasTop()) {
      heap.add(source);
      if (topIdx != null) {
        heap.add(topIdx);
      }
      pullReferencesFromHeap();
    }
  }
}
