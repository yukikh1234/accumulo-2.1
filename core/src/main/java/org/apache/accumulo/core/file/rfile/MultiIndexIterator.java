
package org.apache.accumulo.core.file.rfile;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.blockfile.impl.CacheProvider;
import org.apache.accumulo.core.file.rfile.MultiLevelIndex.IndexEntry;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.HeapIterator;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;

class MultiIndexIterator extends HeapIterator implements FileSKVIterator {

  private final RFile.Reader source;

  MultiIndexIterator(RFile.Reader source, List<Iterator<IndexEntry>> indexes) {
    super(indexes.size());
    this.source = source;
    indexes.forEach(index -> addSource(new IndexIterator(index)));
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    throw new UnsupportedOperationException("deepCopy is not supported in MultiIndexIterator.");
  }

  @Override
  public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
      IteratorEnvironment env) throws IOException {
    throw new UnsupportedOperationException("init is not supported in MultiIndexIterator.");
  }

  @Override
  public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
      throws IOException {
    throw new UnsupportedOperationException("seek is not supported in MultiIndexIterator.");
  }

  @Override
  public void close() throws IOException {
    source.close();
  }

  @Override
  public void closeDeepCopies() throws IOException {
    throw new UnsupportedOperationException(
        "closeDeepCopies is not supported in MultiIndexIterator.");
  }

  @Override
  public Key getFirstKey() throws IOException {
    throw new UnsupportedOperationException("getFirstKey is not supported in MultiIndexIterator.");
  }

  @Override
  public Key getLastKey() throws IOException {
    throw new UnsupportedOperationException("getLastKey is not supported in MultiIndexIterator.");
  }

  @Override
  public DataInputStream getMetaStore(String name) throws IOException {
    throw new UnsupportedOperationException("getMetaStore is not supported in MultiIndexIterator.");
  }

  @Override
  public long estimateOverlappingEntries(KeyExtent extent) throws IOException {
    throw new UnsupportedOperationException(
        "estimateOverlappingEntries is not supported in MultiIndexIterator.");
  }

  @Override
  public void setInterruptFlag(AtomicBoolean flag) {
    throw new UnsupportedOperationException(
        "setInterruptFlag is not supported in MultiIndexIterator.");
  }

  @Override
  public FileSKVIterator getSample(SamplerConfigurationImpl sampleConfig) {
    throw new UnsupportedOperationException("getSample is not supported in MultiIndexIterator.");
  }

  @Override
  public void setCacheProvider(CacheProvider cacheProvider) {
    source.setCacheProvider(cacheProvider);
  }
}
