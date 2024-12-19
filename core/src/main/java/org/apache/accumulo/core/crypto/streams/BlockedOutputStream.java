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

package org.apache.accumulo.core.crypto.streams;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class BlockedOutputStream extends OutputStream {
  private final int blockSize;
  private final DataOutputStream out;
  private final ByteBuffer bb;

  public BlockedOutputStream(OutputStream out, int blockSize, int bufferSize) {
    validateBufferSize(bufferSize);
    this.out = initializeOutputStream(out);
    this.blockSize = blockSize;
    this.bb = ByteBuffer.allocate(calculateBufferSize(bufferSize, blockSize));
  }

  private void validateBufferSize(int bufferSize) {
    if (bufferSize <= 0) {
      throw new IllegalArgumentException("bufferSize must be greater than 0.");
    }
  }

  private DataOutputStream initializeOutputStream(OutputStream out) {
    if (out instanceof DataOutputStream) {
      return (DataOutputStream) out;
    } else {
      return new DataOutputStream(out);
    }
  }

  private int calculateBufferSize(int bufferSize, int blockSize) {
    int remainder = bufferSize % blockSize;
    if (remainder != 0) {
      remainder = blockSize - remainder;
    }
    return bufferSize + remainder - 4;
  }

  @Override
  public synchronized void flush() throws IOException {
    if (!bb.hasArray()) {
      throw new RuntimeException("BlockedOutputStream has no backing array.");
    }
    int size = bb.position();
    if (size == 0) {
      return;
    }
    out.writeInt(size);
    int totalSize = size + calculatePaddingSize(size);
    bb.position(totalSize);
    out.write(bb.array(), 0, totalSize);
    out.flush();
    bb.rewind();
  }

  private int calculatePaddingSize(int size) {
    int remainder = (size + 4) % blockSize;
    if (remainder != 0) {
      remainder = blockSize - remainder;
    }
    return remainder;
  }

  @Override
  public void write(int b) throws IOException {
    if (bb.remaining() == 0) {
      flush();
    }
    bb.put((byte) b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    while (len >= bb.remaining()) {
      int remaining = bb.remaining();
      bb.put(b, off, remaining);
      flush();
      off += remaining;
      len -= remaining;
    }
    bb.put(b, off, len);
  }

  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void close() throws IOException {
    flush();
    out.close();
  }
}
