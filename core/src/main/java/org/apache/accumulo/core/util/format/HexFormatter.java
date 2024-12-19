
package org.apache.accumulo.core.util.format;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.interpret.ScanInterpreter;
import org.apache.hadoop.io.Text;

/**
 * A simple formatter that prints the row, column family, column qualifier, and value as hex
 */
@Deprecated(since = "2.1.0")
public class HexFormatter implements Formatter, ScanInterpreter {

  private static final char[] HEX_CHARS =
      {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

  private Iterator<Entry<Key,Value>> iter;
  private FormatterConfig config;

  /**
   * Converts a byte array to a hex string, inserting dashes every two characters.
   *
   * @param sb the StringBuilder to append to
   * @param bin the byte array to convert
   */
  private void toHex(StringBuilder sb, byte[] bin) {
    for (int i = 0; i < bin.length; i++) {
      if (i > 0 && i % 2 == 0) {
        sb.append('-');
      }
      sb.append(HEX_CHARS[(bin[i] >>> 4) & 0x0F]);
      sb.append(HEX_CHARS[bin[i] & 0x0F]);
    }
  }

  /**
   * Converts a character to its corresponding hex value.
   *
   * @param b the character to convert
   * @return the hex value
   * @throws IllegalArgumentException if the character is not a valid hex character
   */
  private int fromChar(char b) {
    if (b >= '0' && b <= '9') {
      return b - '0';
    } else if (b >= 'a' && b <= 'f') {
      return b - 'a' + 10;
    }
    throw new IllegalArgumentException("Bad char " + b);
  }

  /**
   * Converts a hex string to a byte array.
   *
   * @param hex the hex string
   * @return the resulting byte array
   * @throws IllegalArgumentException if the hex string is invalid
   */
  private byte[] toBinary(String hex) {
    if (hex == null || hex.isEmpty()) {
      throw new IllegalArgumentException("Invalid hex string");
    }
    hex = hex.replace("-", "");
    byte[] bin = new byte[(hex.length() / 2) + (hex.length() % 2)];
    int j = 0;
    for (int i = 0; i < bin.length; i++) {
      bin[i] = (byte) (fromChar(hex.charAt(j++)) << 4);
      if (j < hex.length()) {
        bin[i] |= (byte) fromChar(hex.charAt(j++));
      }
    }
    return bin;
  }

  @Override
  public boolean hasNext() {
    return iter.hasNext();
  }

  @Override
  public String next() {
    Entry<Key,Value> entry = iter.next();
    StringBuilder sb = new StringBuilder();
    formatKey(sb, entry.getKey());
    formatValue(sb, entry.getValue());
    return sb.toString();
  }

  /**
   * Formats the key components into a hex string.
   *
   * @param sb the StringBuilder to append to
   * @param key the Key object
   */
  private void formatKey(StringBuilder sb, Key key) {
    toHex(sb, key.getRowData().toArray());
    sb.append("  ");
    toHex(sb, key.getColumnFamilyData().toArray());
    sb.append("  ");
    toHex(sb, key.getColumnQualifierData().toArray());
    sb.append(" [");
    sb.append(key.getColumnVisibilityData());
    sb.append("] ");
    if (config.willPrintTimestamps()) {
      sb.append(key.getTimestamp());
      sb.append("  ");
    }
  }

  /**
   * Formats the value into a hex string.
   *
   * @param sb the StringBuilder to append to
   * @param value the Value object
   */
  private void formatValue(StringBuilder sb, Value value) {
    toHex(sb, value.get());
  }

  @Override
  public void remove() {
    iter.remove();
  }

  @Override
  public void initialize(Iterable<Entry<Key,Value>> scanner, FormatterConfig config) {
    this.iter = scanner.iterator();
    this.config = new FormatterConfig(config);
  }

  @Override
  public Text interpretRow(Text row) {
    return new Text(toBinary(row.toString()));
  }

  @Override
  public Text interpretBeginRow(Text row) {
    return interpretRow(row);
  }

  @Override
  public Text interpretEndRow(Text row) {
    return interpretRow(row);
  }

  @Override
  public Text interpretColumnFamily(Text cf) {
    return interpretRow(cf);
  }

  @Override
  public Text interpretColumnQualifier(Text cq) {
    return interpretRow(cq);
  }
}
