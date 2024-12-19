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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;

public final class Utils {

  private Utils() {
    // Prevent instantiation
  }

  public static void writeVInt(DataOutput out, int n) throws IOException {
    writeVLong(out, n);
  }

  public static void writeVLong(DataOutput out, long n) throws IOException {
    if ((n < 128) && (n >= -32)) {
      out.writeByte((int) n);
      return;
    }

    long un = (n < 0) ? ~n : n;
    int len = calculateLength(un);
    int firstByte = (int) (n >> ((len - 1) * 8));
    writeEncodedLong(out, n, len, firstByte);
  }

  private static int calculateLength(long un) {
    return (Long.SIZE - Long.numberOfLeadingZeros(un)) / 8 + 1;
  }

  private static void writeEncodedLong(DataOutput out, long n, int len, int firstByte)
      throws IOException {
    switch (len) {
      case 1:
        firstByte >>= 8;
      case 2:
        if ((firstByte < 20) && (firstByte >= -20)) {
          out.writeByte(firstByte - 52);
          out.writeByte((int) n);
          return;
        }
        firstByte >>= 8;
      case 3:
        if ((firstByte < 16) && (firstByte >= -16)) {
          out.writeByte(firstByte - 88);
          out.writeShort((int) n);
          return;
        }
        firstByte >>= 8;
      case 4:
        if ((firstByte < 8) && (firstByte >= -8)) {
          out.writeByte(firstByte - 112);
          out.writeShort(((int) n) >>> 8);
          out.writeByte((int) n);
          return;
        }
        out.writeByte(len - 129);
        out.writeInt((int) n);
        return;
      case 5:
        out.writeByte(len - 129);
        out.writeInt((int) (n >>> 8));
        out.writeByte((int) n);
        return;
      case 6:
        out.writeByte(len - 129);
        out.writeInt((int) (n >>> 16));
        out.writeShort((int) n);
        return;
      case 7:
        out.writeByte(len - 129);
        out.writeInt((int) (n >>> 24));
        out.writeShort((int) (n >>> 8));
        out.writeByte((int) n);
        return;
      case 8:
        out.writeByte(len - 129);
        out.writeLong(n);
        return;
      default:
        throw new RuntimeException("Internal error");
    }
  }

  public static int readVInt(DataInput in) throws IOException {
    long ret = readVLong(in);
    if ((ret > Integer.MAX_VALUE) || (ret < Integer.MIN_VALUE)) {
      throw new RuntimeException("Number too large to be represented as Integer");
    }
    return (int) ret;
  }

  public static long readVLong(DataInput in) throws IOException {
    int firstByte = in.readByte();
    if (firstByte >= -32) {
      return firstByte;
    }
    return readLongValue(in, firstByte);
  }

  private static long readLongValue(DataInput in, int firstByte) throws IOException {
    switch ((firstByte + 128) / 8) {
      case 11:
      case 10:
      case 9:
      case 8:
      case 7:
        return ((firstByte + 52L) << 8) | in.readUnsignedByte();
      case 6:
      case 5:
      case 4:
      case 3:
        return ((firstByte + 88L) << 16) | in.readUnsignedShort();
      case 2:
      case 1:
        return ((firstByte + 112L) << 24) | (in.readUnsignedShort() << 8) | in.readUnsignedByte();
      case 0:
        int len = firstByte + 129;
        return readLongByLength(in, len);
      default:
        throw new RuntimeException("Internal error");
    }
  }

  private static long readLongByLength(DataInput in, int len) throws IOException {
    switch (len) {
      case 4:
        return in.readInt();
      case 5:
        return ((long) in.readInt()) << 8 | in.readUnsignedByte();
      case 6:
        return ((long) in.readInt()) << 16 | in.readUnsignedShort();
      case 7:
        return ((long) in.readInt()) << 24 | (in.readUnsignedShort() << 8) | in.readUnsignedByte();
      case 8:
        return in.readLong();
      default:
        throw new IOException("Corrupted VLong encoding");
    }
  }

  public static void writeString(DataOutput out, String s) throws IOException {
    if (s != null) {
      Text text = new Text(s);
      byte[] buffer = text.getBytes();
      int len = text.getLength();
      writeVInt(out, len);
      out.write(buffer, 0, len);
    } else {
      writeVInt(out, -1);
    }
  }

  public static String readString(DataInput in) throws IOException {
    int length = readVInt(in);
    if (length == -1) {
      return null;
    }
    byte[] buffer = new byte[length];
    in.readFully(buffer);
    return Text.decode(buffer);
  }

  public static final class Version implements Comparable<Version> {
    private final short major;
    private final short minor;

    public Version(DataInput in) throws IOException {
      major = in.readShort();
      minor = in.readShort();
    }

    public Version(short major, short minor) {
      this.major = major;
      this.minor = minor;
    }

    public void write(DataOutput out) throws IOException {
      out.writeShort(major);
      out.writeShort(minor);
    }

    public static int size() {
      return (Short.SIZE + Short.SIZE) / Byte.SIZE;
    }

    @Override
    public String toString() {
      return new StringBuilder("v").append(major).append(".").append(minor).toString();
    }

    public boolean compatibleWith(Version other) {
      return major == other.major;
    }

    @Override
    public int compareTo(Version that) {
      if (major != that.major) {
        return major - that.major;
      }
      return minor - that.minor;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof Version)) {
        return false;
      }
      return compareTo((Version) other) == 0;
    }

    @Override
    public int hashCode() {
      return ((major << 16) + minor);
    }
  }
}
