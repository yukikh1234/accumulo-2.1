
package org.apache.accumulo.core.util.format;

import java.text.DateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

public class DefaultFormatter implements Formatter {
  private Iterator<Entry<Key,Value>> si;
  protected FormatterConfig config;

  /** Used as default DateFormat for some static methods */
  private static final ThreadLocal<DateFormat> formatter =
      DateFormatSupplier.createDefaultFormatSupplier();

  @Override
  public void initialize(Iterable<Entry<Key,Value>> scanner, FormatterConfig config) {
    checkState(false);
    si = scanner.iterator();
    this.config = new FormatterConfig(config);
  }

  @Override
  public boolean hasNext() {
    checkState(true);
    return si.hasNext();
  }

  @Override
  public String next() {
    checkState(true);
    return formatEntry(si.next());
  }

  @Override
  public void remove() {
    checkState(true);
    si.remove();
  }

  protected void checkState(boolean expectInitialized) {
    if (expectInitialized && si == null) {
      throw new IllegalStateException("Not initialized");
    }
    if (!expectInitialized && si != null) {
      throw new IllegalStateException("Already initialized");
    }
  }

  public static String formatEntry(Entry<Key,Value> entry, boolean showTimestamps) {
    DateFormat timestampFormat = showTimestamps ? formatter.get() : null;
    return formatEntry(entry, timestampFormat);
  }

  private static final ThreadLocal<Date> tmpDate = ThreadLocal.withInitial(Date::new);

  public static String formatEntry(Entry<Key,Value> entry, DateFormat timestampFormat) {
    StringBuilder sb = new StringBuilder();
    Key key = entry.getKey();
    Text buffer = new Text();

    appendKeyComponents(sb, key, buffer);

    if (timestampFormat != null) {
      appendTimestamp(sb, entry, timestampFormat);
    }

    appendValue(sb, entry);

    return sb.toString();
  }

  public String formatEntry(Entry<Key,Value> entry) {
    return formatEntry(entry, this.config);
  }

  public static String formatEntry(Entry<Key,Value> entry, FormatterConfig config) {
    StringBuilder sb = new StringBuilder();
    Key key = entry.getKey();
    Text buffer = new Text();

    final int shownLength = config.getShownLength();

    appendKeyComponents(sb, key, buffer, shownLength);

    if (config.willPrintTimestamps() && config.getDateFormatSupplier() != null) {
      appendTimestamp(sb, entry, config.getDateFormatSupplier().get());
    }

    appendValue(sb, entry, shownLength);

    return sb.toString();
  }

  private static void appendKeyComponents(StringBuilder sb, Key key, Text buffer) {
    appendText(sb, key.getRow(buffer)).append(" ");
    appendText(sb, key.getColumnFamily(buffer)).append(":");
    appendText(sb, key.getColumnQualifier(buffer)).append(" ");
    sb.append(new ColumnVisibility(key.getColumnVisibility(buffer)));
  }

  private static void appendKeyComponents(StringBuilder sb, Key key, Text buffer, int shownLength) {
    appendText(sb, key.getRow(buffer), shownLength).append(" ");
    appendText(sb, key.getColumnFamily(buffer), shownLength).append(":");
    appendText(sb, key.getColumnQualifier(buffer), shownLength).append(" ");
    sb.append(new ColumnVisibility(key.getColumnVisibility(buffer)));
  }

  private static void appendTimestamp(StringBuilder sb, Entry<Key,Value> entry,
      DateFormat timestampFormat) {
    tmpDate.get().setTime(entry.getKey().getTimestamp());
    sb.append(" ").append(timestampFormat.format(tmpDate.get()));
  }

  private static void appendValue(StringBuilder sb, Entry<Key,Value> entry) {
    Value value = entry.getValue();
    if (value != null && value.getSize() > 0) {
      sb.append("\t");
      appendValue(sb, value);
    }
  }

  private static void appendValue(StringBuilder sb, Entry<Key,Value> entry, int shownLength) {
    Value value = entry.getValue();
    if (value != null && value.getSize() > 0) {
      sb.append("\t");
      appendValue(sb, value, shownLength);
    }
  }

  static StringBuilder appendText(StringBuilder sb, Text t) {
    return appendBytes(sb, t.getBytes(), 0, t.getLength());
  }

  public static StringBuilder appendText(StringBuilder sb, Text t, int shownLength) {
    return appendBytes(sb, t.getBytes(), 0, t.getLength(), shownLength);
  }

  static StringBuilder appendValue(StringBuilder sb, Value value) {
    return appendBytes(sb, value.get(), 0, value.get().length);
  }

  static StringBuilder appendValue(StringBuilder sb, Value value, int shownLength) {
    return appendBytes(sb, value.get(), 0, value.get().length, shownLength);
  }

  static StringBuilder appendBytes(StringBuilder sb, byte[] ba, int offset, int len) {
    for (int i = 0; i < len; i++) {
      int c = 0xff & ba[offset + i];
      if (c == '\\') {
        sb.append("\\\\");
      } else if (c >= 32 && c <= 126) {
        sb.append((char) c);
      } else {
        sb.append("\\x").append(String.format("%02X", c));
      }
    }
    return sb;
  }

  static StringBuilder appendBytes(StringBuilder sb, byte[] ba, int offset, int len,
      int shownLength) {
    int length = Math.min(len, shownLength);
    return appendBytes(sb, ba, offset, length);
  }

  public Iterator<Entry<Key,Value>> getScannerIterator() {
    return si;
  }

  protected boolean isDoTimestamps() {
    return config.willPrintTimestamps();
  }
}
