
package org.apache.accumulo.core.file.streams;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.accumulo.core.util.ratelimit.NullRateLimiter;
import org.apache.accumulo.core.util.ratelimit.RateLimiter;
import org.apache.hadoop.fs.FSDataOutputStream;

/**
 * A decorator for {@code OutputStream} which limits the rate at which data may be written.
 * Underlying OutputStream is a FSDataOutputStream.
 */
public class RateLimitedOutputStream extends DataOutputStream {
  private final RateLimiter writeLimiter;

  public RateLimitedOutputStream(FSDataOutputStream fsDataOutputStream, RateLimiter writeLimiter) {
    super(fsDataOutputStream);
    this.writeLimiter = writeLimiter == null ? NullRateLimiter.INSTANCE : writeLimiter;
  }

  @Override
  public synchronized void write(int i) throws IOException {
    writeLimiter.acquire(1);
    super.write(i);
  }

  @Override
  public synchronized void write(byte[] buffer, int offset, int length) throws IOException {
    writeLimiter.acquire(length);
    super.write(buffer, offset, length);
  }

  @Override
  public void close() throws IOException {
    super.close();
  }

  public long position() {
    return ((FSDataOutputStream) out).getPos();
  }
}
