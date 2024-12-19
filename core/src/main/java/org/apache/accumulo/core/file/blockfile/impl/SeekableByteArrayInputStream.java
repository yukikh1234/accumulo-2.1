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

package org.apache.accumulo.core.file.blockfile.impl;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.io.InputStream;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class SeekableByteArrayInputStream extends InputStream {

  @SuppressFBWarnings(value = "VO_VOLATILE_REFERENCE_TO_ARRAY",
      justification = "see explanation above")
  private volatile byte[] buffer;
  private int cur;
  private int max;

  @Override
  public int read() {
    return (cur < max) ? (buffer[cur++] & 0xff) : -1;
  }

  @Override
  public int read(byte[] b, int offset, int length) {
    validateReadParameters(b, offset, length);
    if (length == 0) {
      return 0;
    }
    int avail = max - cur;
    if (avail <= 0) {
      return -1;
    }
    if (length > avail) {
      length = avail;
    }
    System.arraycopy(buffer, cur, b, offset, length);
    cur += length;
    return length;
  }

  private void validateReadParameters(byte[] b, int offset, int length) {
    if (b == null) {
      throw new NullPointerException();
    }
    if (length < 0 || offset < 0 || length > b.length - offset) {
      throw new IndexOutOfBoundsException();
    }
  }

  @Override
  public long skip(long requestedSkip) {
    int actualSkip = Math.min((int) requestedSkip, max - cur);
    cur += actualSkip > 0 ? actualSkip : 0;
    return actualSkip;
  }

  @Override
  public int available() {
    return max - cur;
  }

  @Override
  public boolean markSupported() {
    return false;
  }

  @Override
  public void mark(int readAheadLimit) {
    throw new UnsupportedOperationException("Mark operation is not supported.");
  }

  @Override
  public void reset() {
    throw new UnsupportedOperationException("Reset operation is not supported.");
  }

  @Override
  public void close() throws IOException {}

  public SeekableByteArrayInputStream(byte[] buf) {
    requireNonNull(buf, "buf argument was null");
    this.buffer = buf;
    this.cur = 0;
    this.max = buf.length;
  }

  public SeekableByteArrayInputStream(byte[] buf, int maxOffset) {
    requireNonNull(buf, "buf argument was null");
    this.buffer = buf;
    this.cur = 0;
    this.max = maxOffset;
  }

  public void seek(int position) {
    if (position < 0 || position >= max) {
      throw new IllegalArgumentException("position = " + position + " maxOffset = " + max);
    }
    this.cur = position;
  }

  public int getPosition() {
    return this.cur;
  }

  byte[] getBuffer() {
    return buffer;
  }
}
