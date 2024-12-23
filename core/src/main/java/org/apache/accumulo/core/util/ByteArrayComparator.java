
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

import java.io.Serializable;
import java.util.Comparator;

public class ByteArrayComparator implements Comparator<byte[]>, Serializable {
  private static final long serialVersionUID = 1L;

  @Override
  public int compare(byte[] o1, byte[] o2) {
    int lengthComparison = Integer.compare(o1.length, o2.length);
    if (lengthComparison != 0) {
      return lengthComparison;
    }

    for (int i = 0; i < o1.length; i++) {
      int a = (o1[i] & 0xff);
      int b = (o2[i] & 0xff);

      if (a != b) {
        return Integer.compare(a, b);
      }
    }

    return 0;
  }
}
