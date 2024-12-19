
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
package org.apache.accumulo.core.file.keyfunctor;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.util.bloom.Key;

public class RowFunctor implements KeyFunctor {

  @Override
  public Key transform(org.apache.accumulo.core.data.Key acuKey) {
    return new Key(copyRowData(acuKey.getRowData()), 1.0);
  }

  @Override
  public Key transform(Range range) {
    return (isRangeInBloomFilter(range, PartialKey.ROW)) ? transform(range.getStartKey()) : null;
  }

  static boolean isRangeInBloomFilter(Range range, PartialKey keyDepth) {
    if (isInvalidRange(range)) {
      return false;
    }
    if (range.getStartKey().equals(range.getEndKey(), keyDepth)) {
      return true;
    }
    return isKeyRangeMatching(range, keyDepth) && !range.isEndKeyInclusive();
  }

  private static boolean isInvalidRange(Range range) {
    return range.getStartKey() == null || range.getEndKey() == null;
  }

  private static boolean isKeyRangeMatching(Range range, PartialKey keyDepth) {
    return range.getStartKey().followingKey(keyDepth).equals(range.getEndKey(),
        PartialKey.ROW_COLFAM_COLQUAL_COLVIS_TIME);
  }

  private static byte[] copyRowData(ByteSequence row) {
    byte[] keyData = new byte[row.length()];
    System.arraycopy(row.getBackingArray(), 0, keyData, 0, row.length());
    return keyData;
  }
}
