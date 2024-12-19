
package org.apache.accumulo.core.util;

public class UuidUtil {

  /**
   * A fast method for verifying a suffix of a string looks like a uuid.
   *
   * @param offset location where the uuid starts. Its expected the uuid occupies the rest of the
   *        string.
   */
  public static boolean isUUID(String uuid, int offset) {
    if (uuid.length() - offset != 36) {
      return false;
    }
    for (int i = 0; i < 36; i++) {
      char c = uuid.charAt(i + offset);
      if (!isValidUUIDChar(c, i)) {
        return false;
      }
    }
    return true;
  }

  private static boolean isValidUUIDChar(char c, int position) {
    if (position == 8 || position == 13 || position == 18 || position == 23) {
      return c == '-';
    }
    return (c >= '0' && c <= '9') || (c >= 'A' && c <= 'F') || (c >= 'a' && c <= 'f');
  }
}
