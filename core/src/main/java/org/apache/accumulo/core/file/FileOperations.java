
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
package org.apache.accumulo.core.file;

import static org.apache.accumulo.core.file.blockfile.impl.CacheProvider.NULL_PROVIDER;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.file.blockfile.impl.CacheProvider;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.accumulo.core.util.ratelimit.RateLimiter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.FileOutputCommitter;

import com.google.common.cache.Cache;

public abstract class FileOperations {

  private static final String HADOOP_JOBHISTORY_LOCATION = "_logs";

  private static final Set<String> validExtensions =
      Set.of(Constants.MAPFILE_EXTENSION, RFile.EXTENSION);

  private static final Set<String> bulkWorkingFiles =
      Set.of(Constants.BULK_LOAD_MAPPING, Constants.BULK_RENAME_FILE,
          FileOutputCommitter.SUCCEEDED_FILE_NAME, HADOOP_JOBHISTORY_LOCATION);

  public static Set<String> getValidExtensions() {
    return validExtensions;
  }

  public static Set<String> getBulkWorkingFiles() {
    return bulkWorkingFiles;
  }

  public static String getNewFileExtension(AccumuloConfiguration acuconf) {
    return acuconf.get(Property.TABLE_FILE_TYPE);
  }

  public static FileOperations getInstance() {
    return new DispatchingFileFactory();
  }

  protected abstract long getFileSize(FileOptions options) throws IOException;

  protected abstract FileSKVWriter openWriter(FileOptions options) throws IOException;

  protected abstract FileSKVIterator openIndex(FileOptions options) throws IOException;

  protected abstract FileSKVIterator openScanReader(FileOptions options) throws IOException;

  protected abstract FileSKVIterator openReader(FileOptions options) throws IOException;

  public WriterBuilder newWriterBuilder() {
    return new WriterBuilder();
  }

  public IndexReaderBuilder newIndexReaderBuilder() {
    return new IndexReaderBuilder();
  }

  public ScanReaderBuilder newScanReaderBuilder() {
    return new ScanReaderBuilder();
  }

  public ReaderBuilder newReaderBuilder() {
    return new ReaderBuilder();
  }

  public static class FileOptions {
    public final AccumuloConfiguration tableConfiguration;
    public final String filename;
    public final FileSystem fs;
    public final Configuration fsConf;
    public final RateLimiter rateLimiter;
    public final String compression;
    public final FSDataOutputStream outputStream;
    public final boolean enableAccumuloStart;
    public final CacheProvider cacheProvider;
    public final Cache<String,Long> fileLenCache;
    public final boolean seekToBeginning;
    public final CryptoService cryptoService;
    public final Range range;
    public final Set<ByteSequence> columnFamilies;
    public final boolean inclusive;
    public final boolean dropCacheBehind;

    public FileOptions(AccumuloConfiguration tableConfiguration, String filename, FileSystem fs,
        Configuration fsConf, RateLimiter rateLimiter, String compression,
        FSDataOutputStream outputStream, boolean enableAccumuloStart, CacheProvider cacheProvider,
        Cache<String,Long> fileLenCache, boolean seekToBeginning, CryptoService cryptoService,
        Range range, Set<ByteSequence> columnFamilies, boolean inclusive, boolean dropCacheBehind) {
      this.tableConfiguration = tableConfiguration;
      this.filename = filename;
      this.fs = fs;
      this.fsConf = fsConf;
      this.rateLimiter = rateLimiter;
      this.compression = compression;
      this.outputStream = outputStream;
      this.enableAccumuloStart = enableAccumuloStart;
      this.cacheProvider = cacheProvider;
      this.fileLenCache = fileLenCache;
      this.seekToBeginning = seekToBeginning;
      this.cryptoService = Objects.requireNonNull(cryptoService);
      this.range = range;
      this.columnFamilies = columnFamilies;
      this.inclusive = inclusive;
      this.dropCacheBehind = dropCacheBehind;
    }

    public AccumuloConfiguration getTableConfiguration() {
      return tableConfiguration;
    }

    public String getFilename() {
      return filename;
    }

    public FileSystem getFileSystem() {
      return fs;
    }

    public Configuration getConfiguration() {
      return fsConf;
    }

    public RateLimiter getRateLimiter() {
      return rateLimiter;
    }

    public String getCompression() {
      return compression;
    }

    public FSDataOutputStream getOutputStream() {
      return outputStream;
    }

    public boolean isAccumuloStartEnabled() {
      return enableAccumuloStart;
    }

    public CacheProvider getCacheProvider() {
      return cacheProvider;
    }

    public Cache<String,Long> getFileLenCache() {
      return fileLenCache;
    }

    public boolean isSeekToBeginning() {
      return seekToBeginning;
    }

    public CryptoService getCryptoService() {
      return cryptoService;
    }

    public Range getRange() {
      return range;
    }

    public Set<ByteSequence> getColumnFamilies() {
      return columnFamilies;
    }

    public boolean isRangeInclusive() {
      return inclusive;
    }
  }

  public static class FileHelper {
    private AccumuloConfiguration tableConfiguration;
    private String filename;
    private FileSystem fs;
    private Configuration fsConf;
    private RateLimiter rateLimiter;
    private CryptoService cryptoService;
    private boolean dropCacheBehind = false;

    protected FileHelper fs(FileSystem fs) {
      this.fs = Objects.requireNonNull(fs);
      return this;
    }

    protected FileHelper fsConf(Configuration fsConf) {
      this.fsConf = Objects.requireNonNull(fsConf);
      return this;
    }

    protected FileHelper filename(String filename) {
      this.filename = Objects.requireNonNull(filename);
      return this;
    }

    protected FileHelper tableConfiguration(AccumuloConfiguration tableConfiguration) {
      this.tableConfiguration = Objects.requireNonNull(tableConfiguration);
      return this;
    }

    protected FileHelper rateLimiter(RateLimiter rateLimiter) {
      this.rateLimiter = rateLimiter;
      return this;
    }

    protected FileHelper cryptoService(CryptoService cs) {
      this.cryptoService = Objects.requireNonNull(cs);
      return this;
    }

    protected FileHelper dropCacheBehind(boolean drop) {
      this.dropCacheBehind = drop;
      return this;
    }

    protected FileOptions toWriterBuilderOptions(String compression,
        FSDataOutputStream outputStream, boolean startEnabled) {
      return new FileOptions(tableConfiguration, filename, fs, fsConf, rateLimiter, compression,
          outputStream, startEnabled, NULL_PROVIDER, null, false, cryptoService, null, null, true,
          dropCacheBehind);
    }

    protected FileOptions toReaderBuilderOptions(CacheProvider cacheProvider,
        Cache<String,Long> fileLenCache, boolean seekToBeginning) {
      return new FileOptions(tableConfiguration, filename, fs, fsConf, rateLimiter, null, null,
          false, cacheProvider == null ? NULL_PROVIDER : cacheProvider, fileLenCache,
          seekToBeginning, cryptoService, null, null, true, dropCacheBehind);
    }

    protected FileOptions toIndexReaderBuilderOptions(Cache<String,Long> fileLenCache) {
      return new FileOptions(tableConfiguration, filename, fs, fsConf, rateLimiter, null, null,
          false, NULL_PROVIDER, fileLenCache, false, cryptoService, null, null, true,
          dropCacheBehind);
    }

    protected FileOptions toScanReaderBuilderOptions(Range range, Set<ByteSequence> columnFamilies,
        boolean inclusive) {
      return new FileOptions(tableConfiguration, filename, fs, fsConf, rateLimiter, null, null,
          false, NULL_PROVIDER, null, false, cryptoService, range, columnFamilies, inclusive,
          dropCacheBehind);
    }

    protected AccumuloConfiguration getTableConfiguration() {
      return tableConfiguration;
    }
  }

  public class WriterBuilder extends FileHelper implements WriterTableConfiguration {
    private String compression;
    private FSDataOutputStream outputStream;
    private boolean enableAccumuloStart = true;

    public WriterTableConfiguration forOutputStream(String extension,
        FSDataOutputStream outputStream, Configuration fsConf, CryptoService cs) {
      this.outputStream = outputStream;
      filename("foo" + extension).fsConf(fsConf).cryptoService(cs);
      return this;
    }

    public WriterTableConfiguration forFile(String filename, FileSystem fs, Configuration fsConf,
        CryptoService cs) {
      filename(filename).fs(fs).fsConf(fsConf).cryptoService(cs);
      return this;
    }

    @Override
    public WriterBuilder withTableConfiguration(AccumuloConfiguration tableConfiguration) {
      tableConfiguration(tableConfiguration);
      return this;
    }

    public WriterBuilder withStartDisabled() {
      this.enableAccumuloStart = false;
      return this;
    }

    public WriterBuilder withCompression(String compression) {
      this.compression = compression;
      return this;
    }

    public WriterBuilder withRateLimiter(RateLimiter rateLimiter) {
      rateLimiter(rateLimiter);
      return this;
    }

    public WriterBuilder dropCachesBehind() {
      this.dropCacheBehind(true);
      return this;
    }

    public FileSKVWriter build() throws IOException {
      return openWriter(toWriterBuilderOptions(compression, outputStream, enableAccumuloStart));
    }
  }

  public interface WriterTableConfiguration {
    public WriterBuilder withTableConfiguration(AccumuloConfiguration tableConfiguration);
  }

  public class ReaderBuilder extends FileHelper implements ReaderTableConfiguration {
    private CacheProvider cacheProvider;
    private Cache<String,Long> fileLenCache;
    private boolean seekToBeginning = false;

    public ReaderTableConfiguration forFile(String filename, FileSystem fs, Configuration fsConf,
        CryptoService cs) {
      filename(filename).fs(fs).fsConf(fsConf).cryptoService(cs);
      return this;
    }

    @Override
    public ReaderBuilder withTableConfiguration(AccumuloConfiguration tableConfiguration) {
      tableConfiguration(tableConfiguration);
      return this;
    }

    public ReaderBuilder withCacheProvider(CacheProvider cacheProvider) {
      this.cacheProvider = cacheProvider;
      return this;
    }

    public ReaderBuilder withFileLenCache(Cache<String,Long> fileLenCache) {
      this.fileLenCache = fileLenCache;
      return this;
    }

    public ReaderBuilder withRateLimiter(RateLimiter rateLimiter) {
      rateLimiter(rateLimiter);
      return this;
    }

    public ReaderBuilder dropCachesBehind() {
      this.dropCacheBehind(true);
      return this;
    }

    public ReaderBuilder seekToBeginning() {
      seekToBeginning(true);
      return this;
    }

    public ReaderBuilder seekToBeginning(boolean seekToBeginning) {
      this.seekToBeginning = seekToBeginning;
      return this;
    }

    public FileSKVIterator build() throws IOException {
      return openReader(toReaderBuilderOptions(cacheProvider, fileLenCache, seekToBeginning));
    }
  }

  public interface ReaderTableConfiguration {
    ReaderBuilder withTableConfiguration(AccumuloConfiguration tableConfiguration);
  }

  public class IndexReaderBuilder extends FileHelper implements IndexReaderTableConfiguration {

    private Cache<String,Long> fileLenCache = null;

    public IndexReaderTableConfiguration forFile(String filename, FileSystem fs,
        Configuration fsConf, CryptoService cs) {
      filename(filename).fs(fs).fsConf(fsConf).cryptoService(cs);
      return this;
    }

    @Override
    public IndexReaderBuilder withTableConfiguration(AccumuloConfiguration tableConfiguration) {
      tableConfiguration(tableConfiguration);
      return this;
    }

    public IndexReaderBuilder withFileLenCache(Cache<String,Long> fileLenCache) {
      this.fileLenCache = fileLenCache;
      return this;
    }

    public FileSKVIterator build() throws IOException {
      return openIndex(toIndexReaderBuilderOptions(fileLenCache));
    }
  }

  public interface IndexReaderTableConfiguration {
    IndexReaderBuilder withTableConfiguration(AccumuloConfiguration tableConfiguration);
  }

  public class ScanReaderBuilder extends FileHelper implements ScanReaderTableConfiguration {
    private Range range;
    private Set<ByteSequence> columnFamilies;
    private boolean inclusive;

    public ScanReaderTableConfiguration forFile(String filename, FileSystem fs,
        Configuration fsConf, CryptoService cs) {
      filename(filename).fs(fs).fsConf(fsConf).cryptoService(cs);
      return this;
    }

    @Override
    public ScanReaderBuilder withTableConfiguration(AccumuloConfiguration tableConfiguration) {
      tableConfiguration(tableConfiguration);
      return this;
    }

    public ScanReaderBuilder overRange(Range range, Set<ByteSequence> columnFamilies,
        boolean inclusive) {
      Objects.requireNonNull(range);
      Objects.requireNonNull(columnFamilies);
      this.range = range;
      this.columnFamilies = columnFamilies;
      this.inclusive = inclusive;
      return this;
    }

    public FileSKVIterator build() throws IOException {
      return openScanReader(toScanReaderBuilderOptions(range, columnFamilies, inclusive));
    }
  }

  public interface ScanReaderTableConfiguration {
    ScanReaderBuilder withTableConfiguration(AccumuloConfiguration tableConfiguration);
  }
}
