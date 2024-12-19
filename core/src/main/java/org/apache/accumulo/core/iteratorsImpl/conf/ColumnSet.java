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

package org.apache.accumulo.core.iteratorsImpl.conf;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.iteratorsImpl.conf.ColumnUtil.ColFamHashKey;
import org.apache.accumulo.core.iteratorsImpl.conf.ColumnUtil.ColHashKey;
import org.apache.accumulo.core.util.Pair;
import org.apache.hadoop.io.Text;

public class ColumnSet {
  private final Set<ColFamHashKey> objectsCF;
  private final Set<ColHashKey> objectsCol;

  private final ColHashKey lookupCol = new ColHashKey();
  private final ColFamHashKey lookupCF = new ColFamHashKey();

  public ColumnSet() {
    objectsCF = new HashSet<>();
    objectsCol = new HashSet<>();
  }

  public ColumnSet(Collection<String> objectStrings) {
    this();
    objectStrings.stream().map(ColumnSet::decodeColumns).forEach(pcic -> {
      if (pcic.getSecond() == null) {
        add(pcic.getFirst());
      } else {
        add(pcic.getFirst(), pcic.getSecond());
      }
    });
  }

  protected void add(Text colf) {
    objectsCF.add(new ColFamHashKey(new Text(colf)));
  }

  protected void add(Text colf, Text colq) {
    objectsCol.add(new ColHashKey(colf, colq));
  }

  public boolean contains(Key key) {
    lookupCol.set(key);
    if (!objectsCol.isEmpty() && objectsCol.contains(lookupCol)) {
      return true;
    }
    lookupCF.set(key);
    return !objectsCF.isEmpty() && objectsCF.contains(lookupCF);
  }

  public boolean isEmpty() {
    return objectsCol.isEmpty() && objectsCF.isEmpty();
  }

  public static String encodeColumns(Text columnFamily, Text columnQualifier) {
    StringBuilder sb = new StringBuilder();
    encode(sb, columnFamily);
    if (columnQualifier != null) {
      sb.append(':');
      encode(sb, columnQualifier);
    }
    return sb.toString();
  }

  static void encode(StringBuilder sb, Text t) {
    for (byte b : t.getBytes()) {
      if (isUnreservedChar(b)) {
        sb.append((char) b);
      } else {
        sb.append('%').append(String.format("%02x", b));
      }
    }
  }

  private static boolean isUnreservedChar(int b) {
    return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_'
        || b == '-';
  }

  public static boolean isValidEncoding(String enc) {
    for (char c : enc.toCharArray()) {
      if (!isValidEncodedChar(c)) {
        return false;
      }
    }
    return true;
  }

  private static boolean isValidEncodedChar(char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_'
        || c == '-' || c == ':' || c == '%';
  }

  public static Pair<Text,Text> decodeColumns(String columns) {
    if (!isValidEncoding(columns)) {
      throw new IllegalArgumentException("Invalid encoding " + columns);
    }
    String[] cols = columns.split(":");
    if (cols.length == 1) {
      return new Pair<>(decode(cols[0]), null);
    } else if (cols.length == 2) {
      return new Pair<>(decode(cols[0]), decode(cols[1]));
    } else {
      throw new IllegalArgumentException(columns);
    }
  }

  static Text decode(String s) {
    Text t = new Text();
    byte[] sb = s.getBytes(UTF_8);

    for (int i = 0; i < sb.length; i++) {
      if (sb[i] == '%') {
        if (i + 2 < sb.length) {
          byte b =
              (byte) Integer.parseInt(new String(new byte[] {sb[i + 1], sb[i + 2]}, UTF_8), 16);
          t.append(new byte[] {b}, 0, 1);
          i += 2;
        } else {
          throw new IllegalArgumentException("Invalid encoding in string: " + s);
        }
      } else {
        t.append(new byte[] {sb[i]}, 0, 1);
      }
    }
    return t;
  }
}
