
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
package org.apache.accumulo.core.util;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;

public class ByteArraySet extends TreeSet<byte[]> {

  private static final long serialVersionUID = 1L;

  public ByteArraySet() {
    super(new ByteArrayComparator());
  }

  public ByteArraySet(Collection<? extends byte[]> c) {
    this();
    addAll(c);
  }

  public static ByteArraySet fromStrings(Collection<String> c) {
    List<byte[]> byteArrayList = convertStringsToByteArray(c);
    return new ByteArraySet(byteArrayList);
  }

  public static ByteArraySet fromStrings(String... c) {
    return fromStrings(Arrays.asList(c));
  }

  private static List<byte[]> convertStringsToByteArray(Collection<String> c) {
    List<byte[]> byteArrayList = new ArrayList<>();
    for (String s : c) {
      byteArrayList.add(s.getBytes(UTF_8));
    }
    return byteArrayList;
  }
}
