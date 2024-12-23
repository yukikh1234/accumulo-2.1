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

package org.apache.accumulo.core.file.rfile;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;

import org.apache.accumulo.core.client.sample.Sampler;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.file.FileSKVWriter;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile.CachableBuilder;
import org.apache.accumulo.core.file.rfile.bcfile.BCFile;
import org.apache.accumulo.core.sample.impl.SamplerConfigurationImpl;
import org.apache.accumulo.core.sample.impl.SamplerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class RFileOperations extends FileOperations {

  private static final Logger LOG = LoggerFactory.getLogger(RFileOperations.class);
  private static final Collection<ByteSequence> EMPTY_CF_SET = Collections.emptySet();

  private static RFile.Reader getReader(FileOptions options) throws IOException {
    CachableBuilder cb = new CachableBuilder()
        .fsPath(options.getFileSystem(), new Path(options.getFilename()), options.dropCacheBehind)
        .conf(options.getConfiguration()).fileLen(options.getFileLenCache())
        .cacheProvider(options.cacheProvider).readLimiter(options.getRateLimiter())
        .cryptoService(options.getCryptoService());
    return new RFile.Reader(cb);
  }

  @Override
  protected long getFileSize(FileOptions options) throws IOException {
    return options.getFileSystem().getFileStatus(new Path(options.getFilename())).getLen();
  }

  @Override
  protected FileSKVIterator openIndex(FileOptions options) throws IOException {
    return getReader(options).getIndex();
  }

  @Override
  protected FileSKVIterator openReader(FileOptions options) throws IOException {
    RFile.Reader reader = getReader(options);
    if (options.isSeekToBeginning()) {
      reader.seek(new Range((Key) null, null), EMPTY_CF_SET, false);
    }
    return reader;
  }

  @Override
  protected FileSKVIterator openScanReader(FileOptions options) throws IOException {
    RFile.Reader reader = getReader(options);
    reader.seek(options.getRange(), options.getColumnFamilies(), options.isRangeInclusive());
    return reader;
  }

  @Override
  protected FileSKVWriter openWriter(FileOptions options) throws IOException {
    AccumuloConfiguration acuconf = options.getTableConfiguration();
    long blockSize = getValidatedBlockSize(acuconf, Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE);
    long indexBlockSize =
        getValidatedBlockSize(acuconf, Property.TABLE_FILE_COMPRESSED_BLOCK_SIZE_INDEX);

    SamplerConfigurationImpl samplerConfig = SamplerConfigurationImpl.newSamplerConfig(acuconf);
    Sampler sampler = getSampler(samplerConfig, acuconf, options);

    String compression = getCompression(options);

    FSDataOutputStream outputStream = options.getOutputStream();
    Configuration conf = options.getConfiguration();

    if (outputStream == null) {
      outputStream = createOutputStream(options, acuconf, conf);
    }

    BCFile.Writer _cbw = new BCFile.Writer(outputStream, options.getRateLimiter(), compression,
        conf, options.cryptoService);

    return new RFile.Writer(_cbw, (int) blockSize, (int) indexBlockSize, samplerConfig, sampler);
  }

  private long getValidatedBlockSize(AccumuloConfiguration acuconf, Property property) {
    long blockSize = acuconf.getAsBytes(property);
    Preconditions.checkArgument((blockSize < Integer.MAX_VALUE && blockSize > 0),
        property.name() + " must be greater than 0 and less than " + Integer.MAX_VALUE);
    return blockSize;
  }

  private Sampler getSampler(SamplerConfigurationImpl samplerConfig, AccumuloConfiguration acuconf,
      FileOptions options) {
    if (samplerConfig != null) {
      return SamplerFactory.newSampler(samplerConfig, acuconf, options.isAccumuloStartEnabled());
    }
    return null;
  }

  private String getCompression(FileOptions options) {
    String compression = options.getCompression();
    if (compression == null) {
      compression = options.getTableConfiguration().get(Property.TABLE_FILE_COMPRESSION_TYPE);
    }
    return compression;
  }

  private FSDataOutputStream createOutputStream(FileOptions options, AccumuloConfiguration acuconf,
      Configuration conf) throws IOException {
    int hrep = conf.getInt("dfs.replication", 3);
    int trep = acuconf.getCount(Property.TABLE_FILE_REPLICATION);
    int rep = (trep > 0 && trep != hrep) ? trep : hrep;

    long hblock = conf.getLong("dfs.block.size", 1 << 26);
    long tblock = acuconf.getAsBytes(Property.TABLE_FILE_BLOCK_SIZE);
    long block = (tblock > 0) ? tblock : hblock;

    int bufferSize = conf.getInt("io.file.buffer.size", 4096);

    String file = options.getFilename();
    FileSystem fs = options.getFileSystem();

    FSDataOutputStream outputStream;
    if (options.dropCacheBehind) {
      EnumSet<CreateFlag> set = EnumSet.of(CreateFlag.SYNC_BLOCK, CreateFlag.CREATE);
      outputStream = fs.create(new Path(file), FsPermission.getDefault(), set, bufferSize,
          (short) rep, block, null);
      setDropBehind(options, outputStream);
    } else {
      outputStream = fs.create(new Path(file), false, bufferSize, (short) rep, block);
    }
    return outputStream;
  }

  private void setDropBehind(FileOptions options, FSDataOutputStream outputStream) {
    try {
      outputStream.setDropBehind(Boolean.TRUE);
      LOG.trace("Called setDropBehind(TRUE) for stream writing file {}", options.filename);
    } catch (UnsupportedOperationException e) {
      LOG.debug("setDropBehind not enabled for file: {}", options.filename);
    } catch (IOException e) {
      LOG.debug("IOException setting drop behind for file: {}, msg: {}", options.filename,
          e.getMessage());
    }
  }
}
