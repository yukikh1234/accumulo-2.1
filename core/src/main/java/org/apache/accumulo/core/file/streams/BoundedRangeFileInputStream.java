
package org.apache.accumulo.core.file.streams;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.Seekable;

/**
 * BoundedRangeFileInputStream abstracts a contiguous region of a Hadoop FSDataInputStream as a
 * regular input stream. One can create multiple BoundedRangeFileInputStream on top of the same
 * FSDataInputStream and they would not interfere with each other.
 */
public class BoundedRangeFileInputStream extends InputStream {

  private volatile boolean closed = false;
  private final InputStream in;
  private long pos;
  private long end;
  private long mark;
  private final byte[] oneByte = new byte[1];

  /**
   * Constructor
   *
   * @param in The FSDataInputStream we connect to.
   * @param offset Beginning offset of the region.
   * @param length Length of the region.
   *
   *        The actual length of the region may be smaller if (off_begin + length) goes beyond the
   *        end of FS input stream.
   */
  public <StreamType extends InputStream & Seekable> BoundedRangeFileInputStream(StreamType in,
      long offset, long length) {
    if (offset < 0 || length < 0) {
      throw new IndexOutOfBoundsException("Invalid offset/length: " + offset + "/" + length);
    }

    this.in = in;
    this.pos = offset;
    this.end = offset + length;
    this.mark = -1;
  }

  @Override
  public int available() {
    return (int) (end - pos);
  }

  @Override
  public int read() throws IOException {
    int ret = read(oneByte);
    if (ret == 1) {
      return oneByte[0] & 0xff;
    }
    return -1;
  }

  @Override
  public int read(byte[] b) throws IOException {
    return readIntoBuffer(b, 0, b.length);
  }

  @Override
  public int read(final byte[] b, final int off, int len) throws IOException {
    return readIntoBuffer(b, off, len);
  }

  private int readIntoBuffer(final byte[] b, final int off, int len) throws IOException {
    validateBufferParameters(b, off, len);

    final int n = (int) Math.min(Integer.MAX_VALUE, Math.min(len, (end - pos)));
    if (n == 0) {
      return -1;
    }
    int ret;
    synchronized (in) {
      if (closed) {
        throw new IOException("Stream closed");
      }
      ((Seekable) in).seek(pos);
      ret = in.read(b, off, n);
    }
    if (ret < 0) {
      end = pos;
      return -1;
    }
    pos += ret;
    return ret;
  }

  private void validateBufferParameters(final byte[] b, final int off, int len) {
    if ((off | len | (off + len) | (b.length - (off + len))) < 0) {
      throw new IndexOutOfBoundsException();
    }
  }

  @Override
  public long skip(long n) {
    long len = Math.min(n, end - pos);
    pos += len;
    return len;
  }

  @Override
  public synchronized void mark(int readlimit) {
    mark = pos;
  }

  @Override
  public synchronized void reset() throws IOException {
    if (mark < 0) {
      throw new IOException("Resetting to invalid mark");
    }
    pos = mark;
  }

  @Override
  public boolean markSupported() {
    return true;
  }

  @Override
  public void close() {
    if (!closed) {
      synchronized (in) {
        closed = true;
      }
    }
  }
}
