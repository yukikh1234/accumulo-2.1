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
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
 * either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.iteratorsImpl.system;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.ServerFilter;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

public class ColumnQualifierFilter extends ServerFilter {
  private final HashSet<ByteSequence> columnFamilies;
  private final HashMap<ByteSequence,HashSet<ByteSequence>> columnsQualifiers;

  private ColumnQualifierFilter(SortedKeyValueIterator<Key,Value> iterator, Set<Column> columns) {
    super(iterator);
    this.columnFamilies = new HashSet<>();
    this.columnsQualifiers = new HashMap<>();
    processColumns(columns);
  }

  private ColumnQualifierFilter(SortedKeyValueIterator<Key,Value> iterator,
      HashSet<ByteSequence> columnFamilies,
      HashMap<ByteSequence,HashSet<ByteSequence>> columnsQualifiers) {
    super(iterator);
    this.columnFamilies = columnFamilies;
    this.columnsQualifiers = columnsQualifiers;
  }

  private void processColumns(Set<Column> columns) {
    columns.forEach(col -> {
      if (col.columnQualifier != null) {
        this.columnsQualifiers
            .computeIfAbsent(new ArrayByteSequence(col.columnQualifier), k -> new HashSet<>())
            .add(new ArrayByteSequence(col.columnFamily));
      } else {
        columnFamilies.add(new ArrayByteSequence(col.columnFamily));
      }
    });
  }

  @Override
  public boolean accept(Key key, Value v) {
    if (columnFamilies.contains(key.getColumnFamilyData())) {
      return true;
    }

    HashSet<ByteSequence> cfset = columnsQualifiers.get(key.getColumnQualifierData());
    return cfset != null && cfset.contains(key.getColumnFamilyData());
  }

  @Override
  public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
    return new ColumnQualifierFilter(source.deepCopy(env), columnFamilies, columnsQualifiers);
  }

  public static SortedKeyValueIterator<Key,Value> wrap(SortedKeyValueIterator<Key,Value> source,
      Set<Column> cols) {
    if (containsNonNullQualifiers(cols)) {
      return new ColumnQualifierFilter(source, cols);
    } else {
      return source;
    }
  }

  private static boolean containsNonNullQualifiers(Set<Column> cols) {
    for (Column col : cols) {
      if (col.getColumnQualifier() != null) {
        return true;
      }
    }
    return false;
  }
}
