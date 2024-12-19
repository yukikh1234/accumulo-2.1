
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

public class ColumnQualifierFunctor implements KeyFunctor {

  @Override
  public Key transform(org.apache.accumulo.core.data.Key acuKey) {
    byte[] keyData = extractKeyData(acuKey);
    return new Key(keyData, 1.0);
  }

  private byte[] extractKeyData(org.apache.accumulo.core.data.Key acuKey) {
    ByteSequence row = acuKey.getRowData();
    ByteSequence cf = acuKey.getColumnFamilyData();
    ByteSequence cq = acuKey.getColumnQualifierData();

    byte[] keyData = new byte[row.length() + cf.length() + cq.length()];
    int offset = 0;

    offset = copyByteSequence(row, keyData, offset);
    offset = copyByteSequence(cf, keyData, offset);
    copyByteSequence(cq, keyData, offset);

    return keyData;
  }

  private int copyByteSequence(ByteSequence seq, byte[] dest, int offset) {
    System.arraycopy(seq.getBackingArray(), seq.offset(), dest, offset, seq.length());
    return offset + seq.length();
  }

  @Override
  public Key transform(Range range) {
    return RowFunctor.isRangeInBloomFilter(range, PartialKey.ROW_COLFAM_COLQUAL)
        ? transform(range.getStartKey()) : null;
  }
}
