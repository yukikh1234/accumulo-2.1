
package org.apache.accumulo.core.file.rfile.bcfile;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;

public class IdentityCodec implements CompressionCodec {

  public static class IdentityCompressor implements Compressor {

    private boolean finish;
    private boolean finished;
    private int nread;
    private int nwrite;
    private byte[] userBuf;
    private int userBufOff;
    private int userBufLen;

    @Override
    public int compress(byte[] b, int off, int len) throws IOException {
      int n = copyData(b, off, len);
      updateStateAfterCompress(n);
      return n;
    }

    private int copyData(byte[] b, int off, int len) {
      int n = Math.min(len, userBufLen);
      if (userBuf != null && b != null) {
        System.arraycopy(userBuf, userBufOff, b, off, n);
      }
      userBufOff += n;
      userBufLen -= n;
      nwrite += n;
      return n;
    }

    private void updateStateAfterCompress(int n) {
      if (finish && userBufLen <= 0) {
        finished = true;
      }
    }

    @Override
    public void end() {}

    @Override
    public void finish() {
      finish = true;
    }

    @Override
    public boolean finished() {
      return finished;
    }

    @Override
    public long getBytesRead() {
      return nread;
    }

    @Override
    public long getBytesWritten() {
      return nwrite;
    }

    @Override
    public boolean needsInput() {
      return userBufLen <= 0;
    }

    @Override
    public void reset() {
      resetState();
    }

    private void resetState() {
      finish = false;
      finished = false;
      nread = 0;
      nwrite = 0;
      userBuf = null;
      userBufOff = 0;
      userBufLen = 0;
    }

    @Override
    public void setDictionary(byte[] b, int off, int len) {}

    @Override
    public void setInput(byte[] b, int off, int len) {
      validateInput(b, off, len);
      nread += len;
      userBuf = b;
      userBufOff = off;
      userBufLen = len;
    }

    private void validateInput(byte[] b, int off, int len) {
      if (b == null || off < 0 || len < 0 || off + len > b.length) {
        throw new IllegalArgumentException("Invalid input parameters");
      }
    }

    @Override
    public void reinit(Configuration conf) {}

  }

  public static class IdentityDecompressor implements Decompressor {

    private boolean finish;
    private boolean finished;
    private int nread;
    private int nwrite;
    private byte[] userBuf;
    private int userBufOff;
    private int userBufLen;

    @Override
    public int decompress(byte[] b, int off, int len) throws IOException {
      int n = copyData(b, off, len);
      updateStateAfterDecompress(n);
      return n;
    }

    private int copyData(byte[] b, int off, int len) {
      int n = Math.min(len, userBufLen);
      if (userBuf != null && b != null) {
        System.arraycopy(userBuf, userBufOff, b, off, n);
      }
      userBufOff += n;
      userBufLen -= n;
      nwrite += n;
      return n;
    }

    private void updateStateAfterDecompress(int n) {
      if (finish && userBufLen <= 0) {
        finished = true;
      }
    }

    @Override
    public void end() {}

    @Override
    public boolean finished() {
      return finished;
    }

    public long getBytesRead() {
      return nread;
    }

    public long getBytesWritten() {
      return nwrite;
    }

    @Override
    public boolean needsDictionary() {
      return false;
    }

    @Override
    public boolean needsInput() {
      return userBufLen <= 0;
    }

    @Override
    public void reset() {
      resetState();
    }

    private void resetState() {
      finish = false;
      finished = false;
      nread = 0;
      nwrite = 0;
      userBuf = null;
      userBufOff = 0;
      userBufLen = 0;
    }

    @Override
    public void setDictionary(byte[] b, int off, int len) {}

    @Override
    public void setInput(byte[] b, int off, int len) {
      validateInput(b, off, len);
      nread += len;
      userBuf = b;
      userBufOff = off;
      userBufLen = len;
    }

    private void validateInput(byte[] b, int off, int len) {
      if (b == null || off < 0 || len < 0 || off + len > b.length) {
        throw new IllegalArgumentException("Invalid input parameters");
      }
    }

    @Override
    public int getRemaining() {
      return 0;
    }

  }

  public static class IdentityCompressionInputStream extends CompressionInputStream {

    protected IdentityCompressionInputStream(InputStream in) throws IOException {
      super(in);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      return in.read(b, off, len);
    }

    @Override
    public void resetState() throws IOException {}

    @Override
    public int read() throws IOException {
      return in.read();
    }

  }

  public static class IdentityCompressionOutputStream extends CompressionOutputStream {

    public IdentityCompressionOutputStream(OutputStream out) {
      super(out);
    }

    @Override
    public void finish() throws IOException {}

    @Override
    public void resetState() throws IOException {}

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      out.write(b, off, len);
    }

    @Override
    public void write(int b) throws IOException {
      out.write(b);
    }
  }

  @Override
  public Compressor createCompressor() {
    return new IdentityCompressor();
  }

  @Override
  public Decompressor createDecompressor() {
    return new IdentityDecompressor();
  }

  @Override
  public CompressionInputStream createInputStream(InputStream in) throws IOException {
    return new IdentityCompressionInputStream(in);
  }

  @Override
  public CompressionInputStream createInputStream(InputStream in, Decompressor d)
      throws IOException {
    return new IdentityCompressionInputStream(in);
  }

  @Override
  public CompressionOutputStream createOutputStream(OutputStream os) throws IOException {
    return new IdentityCompressionOutputStream(os);
  }

  @Override
  public CompressionOutputStream createOutputStream(OutputStream os, Compressor c)
      throws IOException {
    return new IdentityCompressionOutputStream(os);
  }

  @Override
  public Class<? extends Compressor> getCompressorType() {
    return IdentityCompressor.class;
  }

  @Override
  public Class<? extends Decompressor> getDecompressorType() {
    return IdentityDecompressor.class;
  }

  @Override
  public String getDefaultExtension() {
    return ".identity";
  }

}
