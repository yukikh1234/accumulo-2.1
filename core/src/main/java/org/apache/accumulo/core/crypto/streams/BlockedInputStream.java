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

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

/**
 * Reader corresponding to BlockedOutputStream. Expects all data to be in the form of size (int)
 * data (size bytes) junk (however many bytes it takes to complete a block)
 */
public class BlockedInputStream extends InputStream {
  private byte[] buffer;
  private int readPos, writePos;
  private final DataInputStream inStream;
  private final int blockSize;
  private boolean isFinished = false;

  public BlockedInputStream(InputStream in, int blockSize, int maxSize) {
    if (blockSize <= 0) {
      throw new IllegalArgumentException("Block size must be greater than zero");
    }
    this.inStream =
        (in instanceof DataInputStream) ? (DataInputStream) in : new DataInputStream(in);
    this.buffer = new byte[maxSize];
    this.readPos = 0;
    this.writePos = -1;
    this.blockSize = blockSize;
  }

  @Override
  public int read() throws IOException {
    if (getRemainingBytes() > 0) {
      return buffer[incrementReadPosition(1)] & 0xFF;
    }
    return -1;
  }

  private int incrementReadPosition(int increment) {
    int currentPosition = readPos;
    readPos += increment;
    if (readPos >= buffer.length) {
      readPos -= buffer.length;
    }
    return currentPosition;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int bytesToCopy = Math.min(len, getRemainingBytes());
    if (bytesToCopy > 0) {
      System.arraycopy(buffer, readPos, b, off, bytesToCopy);
      incrementReadPosition(bytesToCopy);
    }
    return bytesToCopy;
  }

  private int getRemainingBytes() throws IOException {
    if (isFinished) {
      return -1;
    }
    if (getAvailableBytes() == 0) {
      loadBuffer();
    }
    return getAvailableBytes();
  }

  @Override
  public int available() {
    return getAvailableBytes();
  }

  private int getAvailableBytes() {
    int availableBytes = writePos + 1 - readPos;
    if (availableBytes < 0) {
      availableBytes += buffer.length;
    }
    return Math.min(buffer.length - readPos, availableBytes);
  }

  private boolean loadBuffer() throws IOException {
    if (isFinished) {
      return false;
    }
    int dataSize;
    try {
      dataSize = inStream.readInt();
    } catch (EOFException eof) {
      isFinished = true;
      return false;
    }

    if (dataSize <= 0 || dataSize > buffer.length) {
      isFinished = true;
      return false;
    }

    fillBuffer(dataSize);
    skipJunkData(dataSize);

    return true;
  }

  private void fillBuffer(int dataSize) throws IOException {
    int bufferAvailable = buffer.length - readPos;
    if (dataSize > bufferAvailable) {
      inStream.readFully(buffer, writePos + 1, bufferAvailable);
      inStream.readFully(buffer, 0, dataSize - bufferAvailable);
    } else {
      inStream.readFully(buffer, writePos + 1, dataSize);
    }
    writePos = (writePos + dataSize) % buffer.length;
  }

  private void skipJunkData(int dataSize) throws IOException {
    int remainder = blockSize - ((dataSize + 4) % blockSize);
    if (remainder != blockSize && inStream.available() < remainder) {
      rollbackWrite(dataSize);
      throw new IOException("Insufficient data to skip");
    }
    inStream.skip(remainder);
  }

  private void rollbackWrite(int dataSize) {
    writePos -= dataSize;
    if (writePos < -1) {
      writePos += buffer.length;
    }
  }

  @Override
  public long skip(long n) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void close() throws IOException {
    buffer = null;
    inStream.close();
  }

  @Override
  public synchronized void mark(int readlimit) {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized void reset() throws IOException {
    inStream.reset();
    readPos = 0;
    writePos = -1;
  }

  @Override
  public boolean markSupported() {
    return false;
  }
}
