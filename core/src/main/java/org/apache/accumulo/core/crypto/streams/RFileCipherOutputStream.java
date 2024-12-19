
package org.apache.accumulo.core.crypto.streams;

import java.io.IOException;
import java.io.OutputStream;

import javax.crypto.Cipher;
import javax.crypto.CipherOutputStream;

/**
 * This class extends {@link CipherOutputStream} to include a way to track the number of bytes that
 * have been encrypted by the stream. The write method also includes a mechanism to stop writing and
 * throw an exception if exceeding a maximum number of bytes is attempted.
 */
public class RFileCipherOutputStream extends CipherOutputStream {

  private static final long MAX_OUTPUT_SIZE = 1L << 34; // 16GiB

  private long count = 0;

  /**
   *
   * Constructs a RFileCipherOutputStream
   *
   * @param os the OutputStream object
   * @param c an initialized Cipher object
   */
  public RFileCipherOutputStream(OutputStream os, Cipher c) {
    super(os, c);
  }

  private void checkCount(long increment) throws IOException {
    count += increment;
    if (count > MAX_OUTPUT_SIZE) {
      throw new IOException("Attempt to write " + count + " bytes was made. A maximum of "
          + MAX_OUTPUT_SIZE + " is allowed for an encryption stream.");
    }
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    checkCount(len);
    super.write(b, off, len);
  }

  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(int b) throws IOException {
    checkCount(1);
    super.write(b);
  }
}
