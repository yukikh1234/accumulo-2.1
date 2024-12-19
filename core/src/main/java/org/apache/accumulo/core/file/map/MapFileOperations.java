
package org.apache.accumulo.core.file.map;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.file.blockfile.impl.CacheProvider;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.MapFileIterator;
import org.apache.accumulo.core.iteratorsImpl.system.SequenceFileIterator;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;

public class MapFileOperations extends FileOperations {

  public static class RangeIterator implements FileSKVIterator {

    private final SortedKeyValueIterator<Key,Value> reader;
    private Range range;
    private boolean hasTop;

    public RangeIterator(SortedKeyValueIterator<Key,Value> reader) {
      this.reader = reader;
    }

    @Override
    public void close() throws IOException {
      ((FileSKVIterator) reader).close();
    }

    @Override
    public Key getFirstKey() throws IOException {
      return ((FileSKVIterator) reader).getFirstKey();
    }

    @Override
    public Key getLastKey() throws IOException {
      return ((FileSKVIterator) reader).getLastKey();
    }

    @Override
    public DataInputStream getMetaStore(String name) throws IOException {
      return ((FileSKVIterator) reader).getMetaStore(name);
    }

    @Override
    public long estimateOverlappingEntries(KeyExtent extent) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
      return new RangeIterator(reader.deepCopy(env));
    }

    @Override
    public Key getTopKey() {
      if (!hasTop) {
        throw new IllegalStateException("No top key available");
      }
      return reader.getTopKey();
    }

    @Override
    public Value getTopValue() {
      if (!hasTop) {
        throw new IllegalStateException("No top value available");
      }
      return reader.getTopValue();
    }

    @Override
    public boolean hasTop() {
      return hasTop;
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options,
        IteratorEnvironment env) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void next() throws IOException {
      if (!hasTop) {
        throw new IllegalStateException("No top element to move next");
      }
      reader.next();
      updateHasTop();
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive)
        throws IOException {
      reader.seek(range, columnFamilies, inclusive);
      this.range = range;
      updateHasTop();

      while (hasTop() && range.beforeStartKey(getTopKey())) {
        next();
      }
    }

    private void updateHasTop() {
      hasTop = reader.hasTop() && !range.afterEndKey(reader.getTopKey());
    }

    @Override
    public void closeDeepCopies() throws IOException {
      ((FileSKVIterator) reader).closeDeepCopies();
    }

    @Override
    public void setInterruptFlag(AtomicBoolean flag) {
      ((FileSKVIterator) reader).setInterruptFlag(flag);
    }

    @Override
    public FileSKVIterator getSample(SamplerConfigurationImpl sampleConfig) {
      return ((FileSKVIterator) reader).getSample(sampleConfig);
    }

    @Override
    public void setCacheProvider(CacheProvider cacheProvider) {
      // Intentionally left blank
    }
  }

  @Override
  protected FileSKVIterator openReader(FileOptions options) throws IOException {
    FileSKVIterator iter = new RangeIterator(new MapFileIterator(options.getFileSystem(),
        options.getFilename(), options.getConfiguration()));
    if (options.isSeekToBeginning()) {
      iter.seek(new Range(new Key(), null), new ArrayList<>(), false);
    }
    return iter;
  }

  @Override
  protected FileSKVWriter openWriter(FileOptions options) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected FileSKVIterator openIndex(FileOptions options) throws IOException {
    return new SequenceFileIterator(MapFileUtil.openIndex(options.getConfiguration(),
        options.getFileSystem(), new Path(options.getFilename())), false);
  }

  @Override
  protected long getFileSize(FileOptions options) throws IOException {
    return options.getFileSystem()
        .getFileStatus(new Path(options.getFilename() + "/" + MapFile.DATA_FILE_NAME)).getLen();
  }

  @Override
  protected FileSKVIterator openScanReader(FileOptions options) throws IOException {
    MapFileIterator mfIter = new MapFileIterator(options.getFileSystem(), options.getFilename(),
        options.getConfiguration());

    FileSKVIterator iter = new RangeIterator(mfIter);
    iter.seek(options.getRange(), options.getColumnFamilies(), options.isRangeInclusive());

    return iter;
  }
}
