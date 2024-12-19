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

package org.apache.accumulo.core.file.rfile.bcfile;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.accumulo.core.spi.file.rfile.compression.CompressionAlgorithmConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;

public class CompressionAlgorithm extends Configured {

  public static class FinishOnFlushCompressionStream extends FilterOutputStream {

    FinishOnFlushCompressionStream(CompressionOutputStream cout) {
      super(cout);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      out.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
      CompressionOutputStream cout = (CompressionOutputStream) out;
      cout.finish();
      cout.flush();
      cout.resetState();
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(CompressionAlgorithm.class);
  private static final LoadingCache<Map.Entry<CompressionAlgorithm,Integer>,
      CompressionCodec> codecCache =
          CacheBuilder.newBuilder().maximumSize(25).build(new CodecCacheLoader());

  protected static final int DATA_IBUF_SIZE = 1024;
  protected static final int DATA_OBUF_SIZE = 4 * 1024;

  private final CompressionAlgorithmConfiguration algorithm;
  private final AtomicBoolean checked = new AtomicBoolean(false);
  private transient CompressionCodec codec = null;

  public CompressionAlgorithm(CompressionAlgorithmConfiguration algorithm, Configuration conf) {
    this.algorithm = algorithm;
    setConf(conf);
    codec = initCodec(checked, algorithm.getDefaultBufferSize(), codec);
  }

  CompressionCodec createNewCodec(int bufferSize) {
    return createNewCodec(algorithm.getCodecClassNameProperty(), algorithm.getCodecClassName(),
        bufferSize, algorithm.getBufferSizeProperty());
  }

  public InputStream createDecompressionStream(InputStream downStream, Decompressor decompressor,
      int downStreamBufferSize) throws IOException {
    validateSupported();
    if (algorithm.cacheCodecsWithNonDefaultSizes()) {
      return createDecompressionStream(downStream, decompressor, downStreamBufferSize,
          algorithm.getDefaultBufferSize(), this, codec);
    } else {
      return bufferStream(
          codec.createInputStream(bufferStream(downStream, downStreamBufferSize), decompressor),
          DATA_IBUF_SIZE);
    }
  }

  private InputStream createDecompressionStream(final InputStream stream,
      final Decompressor decompressor, final int bufferSize, final int defaultBufferSize,
      final CompressionAlgorithm algorithm, CompressionCodec codec) throws IOException {
    if (bufferSize != defaultBufferSize) {
      codec = getCodecFromCache(algorithm, bufferSize);
    }
    return bufferStream(codec.createInputStream(stream, decompressor), DATA_IBUF_SIZE);
  }

  private CompressionCodec getCodecFromCache(CompressionAlgorithm algorithm, int bufferSize)
      throws IOException {
    try {
      return codecCache.get(Maps.immutableEntry(algorithm, bufferSize));
    } catch (ExecutionException e) {
      throw new IOException(e);
    }
  }

  public OutputStream createCompressionStream(OutputStream downStream, Compressor compressor,
      int downStreamBufferSize) throws IOException {
    validateSupported();
    return createFinishedOnFlushCompressionStream(downStream, compressor, downStreamBufferSize);
  }

  private void validateSupported() throws IOException {
    if (!isSupported()) {
      throw new IOException("codec class not specified. Did you forget to set property "
          + algorithm.getCodecClassNameProperty() + "?");
    }
  }

  boolean isSupported() {
    return codec != null;
  }

  CompressionCodec getCodec() {
    return codec;
  }

  public Compressor getCompressor() {
    CompressionCodec codec = getCodec();
    if (codec == null)
      return null;
    Compressor compressor = CodecPool.getCompressor(codec);
    if (compressor != null) {
      validateCompressorState(compressor);
      compressor.reset();
    }
    return compressor;
  }

  private void validateCompressorState(Compressor compressor) {
    if (compressor.finished()) {
      LOG.warn("Compressor obtained from CodecPool already finished()");
    } else {
      LOG.trace("Got a compressor: {}", compressor.hashCode());
    }
  }

  public void returnCompressor(final Compressor compressor) {
    if (compressor != null) {
      LOG.trace("Return a compressor: {}", compressor.hashCode());
      CodecPool.returnCompressor(compressor);
    }
  }

  public Decompressor getDecompressor() {
    CompressionCodec codec = getCodec();
    if (codec == null)
      return null;
    Decompressor decompressor = CodecPool.getDecompressor(codec);
    if (decompressor != null) {
      validateDecompressorState(decompressor);
      decompressor.reset();
    }
    return decompressor;
  }

  private void validateDecompressorState(Decompressor decompressor) {
    if (decompressor.finished()) {
      LOG.warn("Decompressor obtained from CodecPool already finished()");
    } else {
      LOG.trace("Got a decompressor: {}", decompressor.hashCode());
    }
  }

  public void returnDecompressor(final Decompressor decompressor) {
    if (decompressor != null) {
      LOG.trace("Returned a decompressor: {}", decompressor.hashCode());
      CodecPool.returnDecompressor(decompressor);
    }
  }

  public String getName() {
    return algorithm.getName();
  }

  private CompressionCodec initCodec(final AtomicBoolean checked, final int bufferSize,
      final CompressionCodec originalCodec) {
    if (!checked.get()) {
      checked.set(true);
      return createNewCodec(bufferSize);
    }
    return originalCodec;
  }

  private CompressionCodec createNewCodec(final String codecClazzProp, final String defaultClazz,
      final int bufferSize, final String bufferSizeConfigOpt) {
    String clazz = codecClazzProp != null
        ? System.getProperty(codecClazzProp, getConf().get(codecClazzProp, defaultClazz))
        : defaultClazz;
    return instantiateCodec(clazz, bufferSize, bufferSizeConfigOpt);
  }

  private CompressionCodec instantiateCodec(String clazz, int bufferSize,
      String bufferSizeConfigOpt) {
    try {
      LOG.info("Trying to load codec class {}", clazz);
      Configuration config = new Configuration(getConf());
      updateBuffer(config, bufferSizeConfigOpt, bufferSize);
      return (CompressionCodec) ReflectionUtils.newInstance(Class.forName(clazz), config);
    } catch (ClassNotFoundException e) {
      LOG.debug("ClassNotFoundException creating codec class {}", clazz, e);
      return null;
    }
  }

  private OutputStream createFinishedOnFlushCompressionStream(final OutputStream downStream,
      final Compressor compressor, final int downStreamBufferSize) throws IOException {
    OutputStream out = bufferStream(downStream, downStreamBufferSize);
    CompressionOutputStream cos = getCodec().createOutputStream(out, compressor);
    return new BufferedOutputStream(new FinishOnFlushCompressionStream(cos), DATA_OBUF_SIZE);
  }

  private OutputStream bufferStream(final OutputStream stream, final int bufferSize) {
    return bufferSize > 0 ? new BufferedOutputStream(stream, bufferSize) : stream;
  }

  private InputStream bufferStream(final InputStream stream, final int bufferSize) {
    return bufferSize > 0 ? new BufferedInputStream(stream, bufferSize) : stream;
  }

  private void updateBuffer(final Configuration config, final String bufferSizeOpt,
      final int bufferSize) {
    if (bufferSize > 0) {
      config.setInt(bufferSizeOpt, bufferSize);
    }
  }

  private static class CodecCacheLoader
      extends CacheLoader<Map.Entry<CompressionAlgorithm,Integer>,CompressionCodec> {
    @Override
    public CompressionCodec load(Map.Entry<CompressionAlgorithm,Integer> key) {
      return key.getKey().createNewCodec(key.getValue());
    }
  }
}
