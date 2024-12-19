
package org.apache.accumulo.core.util;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.WritableUtils;

/**
 * A utility class for reading and writing bytes to byte buffers without synchronization.
 */
public class UnsynchronizedBuffer {

  /**
   * A byte buffer writer.
   */
  public static class Writer {

    private int offset = 0;
    private byte[] data;

    /**
     * Creates a new writer.
     */
    public Writer() {
      this(64);
    }

    /**
     * Creates a new writer.
     *
     * @param initialCapacity initial byte capacity
     */
    public Writer(int initialCapacity) {
      data = new byte[initialCapacity];
    }

    private void reserve(int length) {
      if (offset + length > data.length) {
        int newSize = UnsynchronizedBuffer.nextArraySize(offset + length);
        byte[] newData = new byte[newSize];
        System.arraycopy(data, 0, newData, 0, offset);
        data = newData;
      }
    }

    public void add(byte[] bytes, int off, int length) {
      reserve(length);
      System.arraycopy(bytes, off, data, offset, length);
      offset += length;
    }

    public void add(boolean b) {
      reserve(1);
      data[offset++] = (byte) (b ? 1 : 0);
    }

    public byte[] toArray() {
      byte[] result = new byte[offset];
      System.arraycopy(data, 0, result, 0, offset);
      return result;
    }

    public ByteBuffer toByteBuffer() {
      return ByteBuffer.wrap(data, 0, offset);
    }

    public void writeVInt(int i) {
      writeVLong(i);
    }

    public void writeVLong(long i) {
      reserve(9);
      offset = UnsynchronizedBuffer.writeVLong(data, offset, i);
    }

    public int size() {
      return offset;
    }
  }

  /**
   * A byte buffer reader.
   */
  public static class Reader {
    private int offset;
    private byte[] data;

    public Reader(byte[] b) {
      this.data = b;
    }

    public Reader(ByteBuffer buffer) {
      if (buffer.hasArray() && buffer.array().length == buffer.arrayOffset() + buffer.limit()) {
        offset = buffer.arrayOffset() + buffer.position();
        data = buffer.array();
      } else {
        offset = 0;
        data = ByteBufferUtil.toBytes(buffer);
      }
    }

    public int readInt() {
      return (data[offset++] << 24) + ((data[offset++] & 255) << 16) + ((data[offset++] & 255) << 8)
          + ((data[offset++] & 255) << 0);
    }

    public long readLong() {
      return (((long) data[offset++] << 56) + ((long) (data[offset++] & 255) << 48)
          + ((long) (data[offset++] & 255) << 40) + ((long) (data[offset++] & 255) << 32)
          + ((long) (data[offset++] & 255) << 24) + ((data[offset++] & 255) << 16)
          + ((data[offset++] & 255) << 8) + ((data[offset++] & 255) << 0));
    }

    public void readBytes(byte[] b) {
      System.arraycopy(data, offset, b, 0, b.length);
      offset += b.length;
    }

    public boolean readBoolean() {
      return (data[offset++] == 1);
    }

    public int readVInt() {
      return (int) readVLong();
    }

    public long readVLong() {
      byte firstByte = data[offset++];
      int len = WritableUtils.decodeVIntSize(firstByte);
      if (len == 1) {
        return firstByte;
      }
      long i = 0;
      for (int idx = 0; idx < len - 1; idx++) {
        i = (i << 8) | (data[offset++] & 0xFF);
      }
      return (WritableUtils.isNegativeVInt(firstByte) ? (i ^ -1L) : i);
    }
  }

  public static int nextArraySize(int i) {
    if (i < 0) {
      throw new IllegalArgumentException();
    }

    if (i > (1 << 30)) {
      return Integer.MAX_VALUE - 8;
    }

    if (i == 0) {
      return 1;
    }

    int ret = i - 1;
    ret |= ret >> 1;
    ret |= ret >> 2;
    ret |= ret >> 4;
    ret |= ret >> 8;
    ret |= ret >> 16;
    return ret + 1;
  }

  public static void writeVInt(DataOutput out, byte[] workBuffer, int i) throws IOException {
    int size = UnsynchronizedBuffer.writeVInt(workBuffer, 0, i);
    out.write(workBuffer, 0, size);
  }

  public static void writeVLong(DataOutput out, byte[] workBuffer, long i) throws IOException {
    int size = UnsynchronizedBuffer.writeVLong(workBuffer, 0, i);
    out.write(workBuffer, 0, size);
  }

  public static int writeVInt(byte[] dest, int offset, int i) {
    return writeVLong(dest, offset, i);
  }

  public static int writeVLong(byte[] dest, int offset, long value) {
    if (value >= -112 && value <= 127) {
      dest[offset++] = (byte) value;
      return offset;
    }

    int len = (value < 0) ? -120 : -112;
    if (value < 0) {
      value ^= -1L;
    }

    long tmp = value;
    while (tmp != 0) {
      tmp >>= 8;
      len--;
    }

    dest[offset++] = (byte) len;
    len = (len < -120) ? -(len + 120) : -(len + 112);

    for (int idx = len; idx != 0; idx--) {
      int shiftbits = (idx - 1) * 8;
      dest[offset++] = (byte) ((value >> shiftbits) & 0xFF);
    }
    return offset;
  }
}
