
package org.apache.accumulo.core.file.rfile;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.function.Supplier;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.util.MutableByteSequence;
import org.apache.accumulo.core.util.UnsynchronizedBuffer;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class RelativeKey implements Writable {

  private static final byte BIT = 0x01;

  private Key key;
  private Key prevKey;

  private byte fieldsSame;
  private byte fieldsPrefixed;

  private static final byte ROW_SAME = BIT << 0;
  private static final byte CF_SAME = BIT << 1;
  private static final byte CQ_SAME = BIT << 2;
  private static final byte CV_SAME = BIT << 3;
  private static final byte TS_SAME = BIT << 4;
  private static final byte DELETED = BIT << 5;
  private static final byte PREFIX_COMPRESSION_ENABLED = (byte) (BIT << 7);

  private static final byte ROW_COMMON_PREFIX = BIT << 0;
  private static final byte CF_COMMON_PREFIX = BIT << 1;
  private static final byte CQ_COMMON_PREFIX = BIT << 2;
  private static final byte CV_COMMON_PREFIX = BIT << 3;
  private static final byte TS_DIFF = BIT << 4;

  int rowCommonPrefixLen;
  int cfCommonPrefixLen;
  int cqCommonPrefixLen;
  int cvCommonPrefixLen;
  long tsDiff;

  public RelativeKey() {}

  public RelativeKey(Key prevKey, Key key) {
    this.key = key;
    fieldsSame = 0;
    fieldsPrefixed = 0;

    if (prevKey != null) {
      rowCommonPrefixLen =
          getCommonPrefixLen(prevKey.getRowData(), key.getRowData(), ROW_SAME, ROW_COMMON_PREFIX);
      cfCommonPrefixLen = getCommonPrefixLen(prevKey.getColumnFamilyData(),
          key.getColumnFamilyData(), CF_SAME, CF_COMMON_PREFIX);
      cqCommonPrefixLen = getCommonPrefixLen(prevKey.getColumnQualifierData(),
          key.getColumnQualifierData(), CQ_SAME, CQ_COMMON_PREFIX);
      cvCommonPrefixLen = getCommonPrefixLen(prevKey.getColumnVisibilityData(),
          key.getColumnVisibilityData(), CV_SAME, CV_COMMON_PREFIX);

      tsDiff = key.getTimestamp() - prevKey.getTimestamp();
      if (tsDiff == 0) {
        fieldsSame |= TS_SAME;
      } else {
        fieldsPrefixed |= TS_DIFF;
      }

      fieldsSame |= fieldsPrefixed == 0 ? 0 : PREFIX_COMPRESSION_ENABLED;
    }

    if (key.isDeleted()) {
      fieldsSame |= DELETED;
    }
  }

  private int getCommonPrefixLen(ByteSequence prev, ByteSequence current, byte fieldBit,
      byte commonPrefix) {
    int commonPrefixLen = getCommonPrefix(prev, current);
    if (commonPrefixLen == -1) {
      fieldsSame |= fieldBit;
    } else if (commonPrefixLen > 1) {
      fieldsPrefixed |= commonPrefix;
    }
    return commonPrefixLen;
  }

  static int getCommonPrefix(ByteSequence prev, ByteSequence cur) {
    if (prev == cur) {
      return -1;
    }
    int maxChecks = Math.min(prev.length(), cur.length());
    int common = 0;
    while (common < maxChecks && prev.byteAt(common) == cur.byteAt(common)) {
      common++;
    }
    return prev.length() == cur.length() ? -1 : common;
  }

  public void setPrevKey(Key pk) {
    this.prevKey = pk;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    fieldsSame = in.readByte();
    fieldsPrefixed =
        (fieldsSame & PREFIX_COMPRESSION_ENABLED) == PREFIX_COMPRESSION_ENABLED ? in.readByte() : 0;

    this.key = new Key(getData(in, ROW_SAME, ROW_COMMON_PREFIX, prevKey::getRowData),
        getData(in, CF_SAME, CF_COMMON_PREFIX, prevKey::getColumnFamilyData),
        getData(in, CQ_SAME, CQ_COMMON_PREFIX, prevKey::getColumnQualifierData),
        getData(in, CV_SAME, CV_COMMON_PREFIX, prevKey::getColumnVisibilityData), getTimestamp(in),
        (fieldsSame & DELETED) == DELETED, false);
    this.prevKey = this.key;
  }

  private byte[] getData(DataInput in, byte fieldBit, byte commonPrefix,
      Supplier<ByteSequence> data) throws IOException {
    if ((fieldsSame & fieldBit) == fieldBit) {
      return data.get().toArray();
    } else if ((fieldsPrefixed & commonPrefix) == commonPrefix) {
      return readPrefix(in, data.get());
    } else {
      return read(in);
    }
  }

  private long getTimestamp(DataInput in) throws IOException {
    if ((fieldsSame & TS_SAME) == TS_SAME) {
      return prevKey.getTimestamp();
    } else if ((fieldsPrefixed & TS_DIFF) == TS_DIFF) {
      return WritableUtils.readVLong(in) + prevKey.getTimestamp();
    } else {
      return WritableUtils.readVLong(in);
    }
  }

  public static class SkippR {
    RelativeKey rk;
    int skipped;
    Key prevKey;

    SkippR(RelativeKey rk, int skipped, Key prevKey) {
      this.rk = rk;
      this.skipped = skipped;
      this.prevKey = prevKey;
    }
  }

  public static SkippR fastSkip(DataInput in, Key seekKey, MutableByteSequence value, Key prevKey,
      Key currKey, int entriesLeft) throws IOException {
    MutableByteSequence row = initSequence(currKey, prevKey, seekKey.getRowData());
    MutableByteSequence cf = initSequence(currKey, prevKey, seekKey.getColumnFamilyData());
    MutableByteSequence cq = initSequence(currKey, prevKey, seekKey.getColumnQualifierData());
    MutableByteSequence cv = initSequence(currKey, prevKey, null);

    int rowCmp = compareSequence(currKey, row, seekKey.getRowData());
    int cfCmp = compareSequence(currKey, cf, seekKey.getColumnFamilyData());
    int cqCmp = compareSequence(currKey, cq, seekKey.getColumnQualifierData());

    long ts = currKey != null ? currKey.getTimestamp() : -1;
    long pts = ts;

    byte fieldsSame = -1;
    byte fieldsPrefixed = 0;
    int count = 0;
    Key newPrevKey = null;

    while (count < entriesLeft && !isKeyFound(rowCmp, cfCmp, cqCmp)) {
      fieldsSame = in.readByte();
      fieldsPrefixed = (fieldsSame & PREFIX_COMPRESSION_ENABLED) == PREFIX_COMPRESSION_ENABLED
          ? in.readByte() : 0;

      boolean changed =
          updateSequences(in, fieldsSame, fieldsPrefixed, row, cf, cq, cv, currKey, prevKey);
      if (changed) {
        rowCmp = row.compareTo(seekKey.getRowData());
        cfCmp = cf.compareTo(seekKey.getColumnFamilyData());
        cqCmp = cq.compareTo(seekKey.getColumnQualifierData());
      }

      if ((fieldsSame & TS_SAME) != TS_SAME) {
        pts = ts;
        ts = (fieldsPrefixed & TS_DIFF) == TS_DIFF ? WritableUtils.readVLong(in) + pts
            : WritableUtils.readVLong(in);
      }

      readValue(in, value);
      count++;
    }

    newPrevKey = determineNewPrevKey(fieldsSame, row, cf, cq, cv, ts, pts, currKey, prevKey, count);
    RelativeKey result = new RelativeKey();
    result.key = new Key(row.getBackingArray(), row.offset(), row.length(), cf.getBackingArray(),
        cf.offset(), cf.length(), cq.getBackingArray(), cq.offset(), cq.length(),
        cv.getBackingArray(), cv.offset(), cv.length(), ts);
    result.key.setDeleted((fieldsSame & DELETED) != 0);
    result.prevKey = result.key;

    return new SkippR(result, count, newPrevKey);
  }

  private static MutableByteSequence initSequence(Key currKey, Key prevKey, ByteSequence seekData) {
    if (currKey != null) {
      return new MutableByteSequence(currKey.getRowData());
    } else {
      return new MutableByteSequence(new byte[64], 0, 0);
    }
  }

  private static int compareSequence(Key currKey, MutableByteSequence seq, ByteSequence seekData) {
    return currKey != null ? seq.compareTo(seekData) : -1;
  }

  private static boolean updateSequences(DataInput in, byte fieldsSame, byte fieldsPrefixed,
      MutableByteSequence row, MutableByteSequence cf, MutableByteSequence cq,
      MutableByteSequence cv, Key currKey, Key prevKey) throws IOException {
    boolean changed = false;
    if ((fieldsSame & ROW_SAME) != ROW_SAME) {
      updateSequence(in, fieldsPrefixed, row, prevKey.getRowData(), ROW_COMMON_PREFIX);
      changed = true;
    }
    if ((fieldsSame & CF_SAME) != CF_SAME) {
      updateSequence(in, fieldsPrefixed, cf, prevKey.getColumnFamilyData(), CF_COMMON_PREFIX);
      changed = true;
    }
    if ((fieldsSame & CQ_SAME) != CQ_SAME) {
      updateSequence(in, fieldsPrefixed, cq, prevKey.getColumnQualifierData(), CQ_COMMON_PREFIX);
      changed = true;
    }
    if ((fieldsSame & CV_SAME) != CV_SAME) {
      updateSequence(in, fieldsPrefixed, cv, prevKey.getColumnVisibilityData(), CV_COMMON_PREFIX);
    }
    return changed;
  }

  private static void updateSequence(DataInput in, byte fieldsPrefixed, MutableByteSequence seq,
      ByteSequence prevData, byte prefixBit) throws IOException {
    MutableByteSequence tmp =
        new MutableByteSequence(seq.getBackingArray(), seq.offset(), seq.length());
    seq = tmp;
    if ((fieldsPrefixed & prefixBit) == prefixBit) {
      readPrefix(in, seq, prevData);
    } else {
      read(in, seq);
    }
  }

  private static boolean isKeyFound(int rowCmp, int cfCmp, int cqCmp) {
    return rowCmp >= 0 && cfCmp >= 0 && cqCmp >= 0;
  }

  private static Key determineNewPrevKey(byte fieldsSame, MutableByteSequence row,
      MutableByteSequence cf, MutableByteSequence cq, MutableByteSequence cv, long ts, long pts,
      Key currKey, Key prevKey, int count) {
    Key newPrevKey;
    if (count > 1) {
      newPrevKey = new Key(
          (fieldsSame & ROW_SAME) == ROW_SAME ? row.getBackingArray() : new byte[64], row.offset(),
          row.length(), (fieldsSame & CF_SAME) == CF_SAME ? cf.getBackingArray() : new byte[64],
          cf.offset(), cf.length(),
          (fieldsSame & CQ_SAME) == CQ_SAME ? cq.getBackingArray() : new byte[64], cq.offset(),
          cq.length(), (fieldsSame & CV_SAME) == CV_SAME ? cv.getBackingArray() : new byte[64],
          cv.offset(), cv.length(), (fieldsSame & TS_SAME) == TS_SAME ? ts : pts);
      newPrevKey.setDeleted((fieldsSame & DELETED) != 0);
    } else if (count == 1) {
      newPrevKey = currKey != null ? currKey : prevKey;
    } else {
      throw new IllegalStateException();
    }
    return newPrevKey;
  }

  private static void read(DataInput in, MutableByteSequence mbseq) throws IOException {
    int len = WritableUtils.readVInt(in);
    read(in, mbseq, len);
  }

  private static void readValue(DataInput in, MutableByteSequence mbseq) throws IOException {
    int len = in.readInt();
    read(in, mbseq, len);
  }

  private static void read(DataInput in, MutableByteSequence mbseqDestination, int len)
      throws IOException {
    if (mbseqDestination.getBackingArray().length < len) {
      mbseqDestination.setArray(new byte[UnsynchronizedBuffer.nextArraySize(len)], 0, 0);
    }

    in.readFully(mbseqDestination.getBackingArray(), 0, len);
    mbseqDestination.setLength(len);
  }

  private static byte[] readPrefix(DataInput in, ByteSequence prefixSource) throws IOException {
    int prefixLen = WritableUtils.readVInt(in);
    int remainingLen = WritableUtils.readVInt(in);
    byte[] data = new byte[prefixLen + remainingLen];
    if (prefixSource.isBackedByArray()) {
      System.arraycopy(prefixSource.getBackingArray(), prefixSource.offset(), data, 0, prefixLen);
    } else {
      byte[] prefixArray = prefixSource.toArray();
      System.arraycopy(prefixArray, 0, data, 0, prefixLen);
    }
    in.readFully(data, prefixLen, remainingLen);
    return data;
  }

  private static void readPrefix(DataInput in, MutableByteSequence dest, ByteSequence prefixSource)
      throws IOException {
    int prefixLen = WritableUtils.readVInt(in);
    int remainingLen = WritableUtils.readVInt(in);
    int len = prefixLen + remainingLen;
    if (dest.getBackingArray().length < len) {
      dest.setArray(new byte[UnsynchronizedBuffer.nextArraySize(len)], 0, 0);
    }
    if (prefixSource.isBackedByArray()) {
      System.arraycopy(prefixSource.getBackingArray(), prefixSource.offset(),
          dest.getBackingArray(), 0, prefixLen);
    } else {
      byte[] prefixArray = prefixSource.toArray();
      System.arraycopy(prefixArray, 0, dest.getBackingArray(), 0, prefixLen);
    }
    in.readFully(dest.getBackingArray(), prefixLen, remainingLen);
    dest.setLength(len);
  }

  private static byte[] read(DataInput in) throws IOException {
    int len = WritableUtils.readVInt(in);
    byte[] data = new byte[len];
    in.readFully(data);
    return data;
  }

  public Key getKey() {
    return key;
  }

  private static void write(DataOutput out, ByteSequence bs) throws IOException {
    WritableUtils.writeVInt(out, bs.length());
    out.write(bs.getBackingArray(), bs.offset(), bs.length());
  }

  private static void writePrefix(DataOutput out, ByteSequence bs, int commonPrefixLength)
      throws IOException {
    WritableUtils.writeVInt(out, commonPrefixLength);
    WritableUtils.writeVInt(out, bs.length() - commonPrefixLength);
    out.write(bs.getBackingArray(), bs.offset() + commonPrefixLength,
        bs.length() - commonPrefixLength);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeByte(fieldsSame);

    if ((fieldsSame & PREFIX_COMPRESSION_ENABLED) == PREFIX_COMPRESSION_ENABLED) {
      out.write(fieldsPrefixed);
    }

    writeData(out, key.getRowData(), ROW_SAME, ROW_COMMON_PREFIX, rowCommonPrefixLen);
    writeData(out, key.getColumnFamilyData(), CF_SAME, CF_COMMON_PREFIX, cfCommonPrefixLen);
    writeData(out, key.getColumnQualifierData(), CQ_SAME, CQ_COMMON_PREFIX, cqCommonPrefixLen);
    writeData(out, key.getColumnVisibilityData(), CV_SAME, CV_COMMON_PREFIX, cvCommonPrefixLen);

    if ((fieldsSame & TS_SAME) == TS_SAME) {} else if ((fieldsPrefixed & TS_DIFF) == TS_DIFF) {
      WritableUtils.writeVLong(out, tsDiff);
    } else {
      WritableUtils.writeVLong(out, key.getTimestamp());
    }
  }

  private void writeData(DataOutput out, ByteSequence data, byte sameBit, byte commonPrefixBit,
      int commonPrefixLen) throws IOException {
    if ((fieldsSame & sameBit)
        == sameBit) {} else if ((fieldsPrefixed & commonPrefixBit) == commonPrefixBit) {
      writePrefix(out, data, commonPrefixLen);
    } else {
      write(out, data);
    }
  }
}
