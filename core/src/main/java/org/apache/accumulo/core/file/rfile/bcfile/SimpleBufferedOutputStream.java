
package org.apache.accumulo.core.file.rfile.bcfile;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * A simplified BufferedOutputStream with borrowed buffer, and allow users to see how much data have
 * been buffered.
 */
class SimpleBufferedOutputStream extends FilterOutputStream {
  protected byte[] buf; // the borrowed buffer
  protected int count = 0; // bytes used in buffer.

  // Constructor
  public SimpleBufferedOutputStream(OutputStream out, byte[] buf) {
    super(out);
    this.buf = buf;
  }

  private void flushBuffer() throws IOException {
    if (count > 0) {
      out.write(buf, 0, count);
      count = 0;
    }
  }

  @Override
  public void write(int b) throws IOException {
    if (count >= buf.length) {
      flushBuffer();
    }
    buf[count++] = (byte) b;
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    if (len >= buf.length) {
      handleLargeWrite(b, off, len);
      return;
    }
    if (len > buf.length - count) {
      flushBuffer();
    }
    bufferWrite(b, off, len);
  }

  private void handleLargeWrite(byte[] b, int off, int len) throws IOException {
    flushBuffer();
    out.write(b, off, len);
  }

  private void bufferWrite(byte[] b, int off, int len) {
    System.arraycopy(b, off, buf, count, len);
    count += len;
  }

  @Override
  public synchronized void flush() throws IOException {
    flushBuffer();
    out.flush();
  }

  // Get the size of internal buffer being used.
  public int size() {
    return count;
  }
}
