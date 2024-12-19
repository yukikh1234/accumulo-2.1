
package org.apache.accumulo.core.iteratorsImpl.system;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * Wraps a SortedKeyValueIterator so that all of its methods are synchronized. The intent is that
 * user iterators which are multi-threaded have the possibility to call parent methods concurrently.
 * The SynchronizedIterators aims to reduce the likelihood of unwanted concurrent access.
 */
public class SynchronizedIterator<K extends WritableComparable<?>,V extends Writable>
    implements SortedKeyValueIterator<K,V> {

  private final SortedKeyValueIterator<K,V> source;

  public SynchronizedIterator(SortedKeyValueIterator<K,V> source) {
    this.source = source;
  }

  @Override
  public synchronized void init(SortedKeyValueIterator<K,V> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    // Consider implementing the method or provide a meaningful message
    throw new UnsupportedOperationException("Init method is not implemented.");
  }

  @Override
  public synchronized boolean hasTop() {
    return source.hasTop();
  }

  @Override
  public synchronized void next() throws IOException {
    source.next();
  }

  @Override
  public synchronized void seek(Range range, Collection<ByteSequence> columnFamilies,
      boolean inclusive) throws IOException {
    source.seek(range, columnFamilies, inclusive);
  }

  @Override
  public synchronized K getTopKey() {
    return source.getTopKey();
  }

  @Override
  public synchronized V getTopValue() {
    return source.getTopValue();
  }

  @Override
  public synchronized SortedKeyValueIterator<K,V> deepCopy(IteratorEnvironment env) {
    return new SynchronizedIterator<>(source.deepCopy(env));
  }
}
