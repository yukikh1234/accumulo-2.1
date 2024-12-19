
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
 * OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.accumulo.core.util.compaction;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import org.apache.accumulo.core.clientImpl.Namespace;
import org.apache.accumulo.core.data.NamespaceId;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.RootTable;
import org.apache.accumulo.core.spi.compaction.CompactionJob;
import org.apache.accumulo.core.spi.compaction.CompactionKind;
import org.apache.accumulo.core.util.Pair;
import org.apache.commons.lang3.Range;

import com.google.common.base.Preconditions;

public class CompactionJobPrioritizer {

  public static final Comparator<CompactionJob> JOB_COMPARATOR =
      Comparator.comparingInt(CompactionJob::getPriority)
          .thenComparingInt(job -> job.getFiles().size()).reversed();

  private static final Map<Pair<TableId,CompactionKind>,Range<Short>> SYSTEM_TABLE_RANGES =
      new HashMap<>();
  private static final Map<Pair<NamespaceId,CompactionKind>,
      Range<Short>> ACCUMULO_NAMESPACE_RANGES = new HashMap<>();

  static final Range<Short> ROOT_TABLE_USER = Range.of((short) 30768, (short) 32767);
  static final Range<Short> ROOT_TABLE_SYSTEM = Range.of((short) 28768, (short) 30767);
  static final Range<Short> METADATA_TABLE_USER = Range.of((short) 26768, (short) 28767);
  static final Range<Short> METADATA_TABLE_SYSTEM = Range.of((short) 24768, (short) 26767);
  static final Range<Short> SYSTEM_NS_USER = Range.of((short) 22768, (short) 24767);
  static final Range<Short> SYSTEM_NS_SYSTEM = Range.of((short) 20768, (short) 22767);
  static final Range<Short> TABLE_OVER_SIZE = Range.of((short) 18768, (short) 20767);
  static final Range<Short> USER_TABLE_USER = Range.of((short) 1, (short) 18767);
  static final Range<Short> USER_TABLE_SYSTEM = Range.of((short) -32768, (short) 0);

  static {
    SYSTEM_TABLE_RANGES.put(new Pair<>(RootTable.ID, CompactionKind.USER), ROOT_TABLE_USER);
    SYSTEM_TABLE_RANGES.put(new Pair<>(RootTable.ID, CompactionKind.SYSTEM), ROOT_TABLE_SYSTEM);
    SYSTEM_TABLE_RANGES.put(new Pair<>(MetadataTable.ID, CompactionKind.USER), METADATA_TABLE_USER);
    SYSTEM_TABLE_RANGES.put(new Pair<>(MetadataTable.ID, CompactionKind.SYSTEM),
        METADATA_TABLE_SYSTEM);
    ACCUMULO_NAMESPACE_RANGES.put(new Pair<>(Namespace.ACCUMULO.id(), CompactionKind.USER),
        SYSTEM_NS_USER);
    ACCUMULO_NAMESPACE_RANGES.put(new Pair<>(Namespace.ACCUMULO.id(), CompactionKind.SYSTEM),
        SYSTEM_NS_SYSTEM);
  }

  public static short createPriority(final NamespaceId nsId, final TableId tableId,
      final CompactionKind kind, final int totalFiles, final int compactingFiles,
      final int maxFilesPerTablet) {

    validateInputs(nsId, tableId, totalFiles, compactingFiles);
    CompactionKind calculationKind = determineCalculationKind(kind);
    Range<Short> range =
        determineRange(nsId, tableId, calculationKind, totalFiles, maxFilesPerTablet);
    Function<Range<Short>,Short> func =
        selectPriorityFunction(totalFiles, compactingFiles, maxFilesPerTablet, calculationKind);

    return func.apply(range);
  }

  private static void validateInputs(NamespaceId nsId, TableId tableId, int totalFiles,
      int compactingFiles) {
    Objects.requireNonNull(nsId, "nsId cannot be null");
    Objects.requireNonNull(tableId, "tableId cannot be null");
    Preconditions.checkArgument(totalFiles >= 0, "totalFiles is negative %s", totalFiles);
    Preconditions.checkArgument(compactingFiles >= 0, "compactingFiles is negative %s",
        compactingFiles);
  }

  private static CompactionKind determineCalculationKind(CompactionKind kind) {
    if (kind == CompactionKind.CHOP) {
      return CompactionKind.USER;
    } else if (kind == CompactionKind.SELECTOR) {
      return CompactionKind.SYSTEM;
    }
    return kind;
  }

  private static Range<Short> determineRange(NamespaceId nsId, TableId tableId,
      CompactionKind calculationKind, int totalFiles, int maxFilesPerTablet) {
    if (Namespace.ACCUMULO.id().equals(nsId)) {
      Range<Short> range = SYSTEM_TABLE_RANGES.get(new Pair<>(tableId, calculationKind));
      if (range == null) {
        range = ACCUMULO_NAMESPACE_RANGES.get(new Pair<>(nsId, calculationKind));
      }
      if (range != null) {
        return range;
      }
    } else {
      return calculateUserTableRange(calculationKind, totalFiles, maxFilesPerTablet);
    }
    throw new IllegalStateException("Error calculating compaction priority for table: " + tableId);
  }

  private static Range<Short> calculateUserTableRange(CompactionKind calculationKind,
      int totalFiles, int maxFilesPerTablet) {
    if (totalFiles > maxFilesPerTablet && calculationKind == CompactionKind.SYSTEM) {
      return TABLE_OVER_SIZE;
    } else if (calculationKind == CompactionKind.SYSTEM) {
      return USER_TABLE_SYSTEM;
    } else {
      return USER_TABLE_USER;
    }
  }

  private static Function<Range<Short>,Short> selectPriorityFunction(int totalFiles,
      int compactingFiles, int maxFilesPerTablet, CompactionKind calculationKind) {
    if (totalFiles > maxFilesPerTablet && calculationKind == CompactionKind.SYSTEM) {
      return f -> (short) Math.min(f.getMaximum(),
          f.getMinimum() + compactingFiles + (totalFiles - maxFilesPerTablet));
    } else {
      return f -> (short) Math.min(f.getMaximum(), f.getMinimum() + totalFiles + compactingFiles);
    }
  }
}
