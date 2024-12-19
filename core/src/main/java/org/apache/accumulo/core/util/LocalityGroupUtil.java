
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

import static java.util.stream.Collectors.toUnmodifiableSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.ConfigurationCopy;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.thrift.TMutation;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class LocalityGroupUtil {

  private static final Logger log = LoggerFactory.getLogger(LocalityGroupUtil.class);

  public static Set<ByteSequence> families(Collection<Column> columns) {
    if (columns.isEmpty()) {
      return Set.of();
    }
    return columns.stream().map(c -> new ArrayByteSequence(c.getColumnFamily()))
        .collect(toUnmodifiableSet());
  }

  public static class LocalityGroupConfigurationError extends AccumuloException {
    private static final long serialVersionUID = 855450342044719186L;

    LocalityGroupConfigurationError(String why) {
      super(why);
    }
  }

  public static boolean isLocalityGroupProperty(String prop) {
    return prop.startsWith(Property.TABLE_LOCALITY_GROUP_PREFIX.getKey())
        || prop.equals(Property.TABLE_LOCALITY_GROUPS.getKey());
  }

  public static void checkLocalityGroups(Map<String,String> config)
      throws LocalityGroupConfigurationError {
    ConfigurationCopy cc = new ConfigurationCopy(config);
    if (cc.get(Property.TABLE_LOCALITY_GROUPS) != null) {
      getLocalityGroups(cc);
    }
  }

  public static Map<String,Set<ByteSequence>>
      getLocalityGroupsIgnoringErrors(AccumuloConfiguration acuconf, TableId tableId) {
    try {
      return getLocalityGroups(acuconf);
    } catch (LocalityGroupConfigurationError | RuntimeException e) {
      log.warn("Failed to get locality group config for tableId:" + tableId
          + ", proceeding without locality groups.", e);
    }

    return Collections.emptyMap();
  }

  public static Map<String,Set<ByteSequence>> getLocalityGroups(AccumuloConfiguration acuconf)
      throws LocalityGroupConfigurationError {
    Map<String,Set<ByteSequence>> result = new HashMap<>();
    populateGroups(acuconf, result);
    validateGroups(result);
    return result;
  }

  private static void populateGroups(AccumuloConfiguration acuconf,
      Map<String,Set<ByteSequence>> result) throws LocalityGroupConfigurationError {
    String[] groups = acuconf.get(Property.TABLE_LOCALITY_GROUPS).split(",");
    for (String group : groups) {
      if (!group.isEmpty()) {
        result.put(group, new HashSet<>());
      }
    }
    HashSet<ByteSequence> all = new HashSet<>();
    for (Entry<String,String> entry : acuconf) {
      String property = entry.getKey();
      String value = entry.getValue();
      String prefix = Property.TABLE_LOCALITY_GROUP_PREFIX.getKey();
      if (property.startsWith(prefix)) {
        String group = property.substring(prefix.length()).split("\\.")[0];
        if (result.containsKey(group)) {
          Set<ByteSequence> colFamsSet = decodeColumnFamilies(value);
          checkForOverlap(all, colFamsSet, group);
          all.addAll(colFamsSet);
          result.put(group, colFamsSet);
        }
      }
    }
  }

  private static void checkForOverlap(HashSet<ByteSequence> all, Set<ByteSequence> colFamsSet,
      String group) throws LocalityGroupConfigurationError {
    if (!Collections.disjoint(all, colFamsSet)) {
      colFamsSet.retainAll(all);
      throw new LocalityGroupConfigurationError("Column families " + colFamsSet + " in group "
          + group + " is already used by another locality group");
    }
  }

  private static void validateGroups(Map<String,Set<ByteSequence>> result)
      throws LocalityGroupConfigurationError {
    for (Entry<String,Set<ByteSequence>> entry : result.entrySet()) {
      if (entry.getValue().isEmpty()) {
        throw new LocalityGroupConfigurationError(
            "Locality group " + entry.getKey() + " specified but not declared");
      }
    }
  }

  public static Set<ByteSequence> decodeColumnFamilies(String colFams)
      throws LocalityGroupConfigurationError {
    HashSet<ByteSequence> colFamsSet = new HashSet<>();
    for (String family : colFams.split(",")) {
      ByteSequence cfbs = decodeColumnFamily(family);
      colFamsSet.add(cfbs);
    }
    return colFamsSet;
  }

  public static ByteSequence decodeColumnFamily(String colFam)
      throws LocalityGroupConfigurationError {
    byte[] output = new byte[colFam.length()];
    int pos = 0;

    for (int i = 0; i < colFam.length(); i++) {
      char c = colFam.charAt(i);

      if (c == '\\') {
        pos = handleEscapeSequence(colFam, output, pos, i);
        i += 2;
      } else {
        output[pos++] = (byte) (0xff & c);
      }
    }

    return new ArrayByteSequence(output, 0, pos);
  }

  private static int handleEscapeSequence(String colFam, byte[] output, int pos, int i)
      throws LocalityGroupConfigurationError {
    if (++i >= colFam.length()) {
      throw new LocalityGroupConfigurationError("Expected 'x' or '\\' after '\\' in " + colFam);
    }

    char nc = colFam.charAt(i);

    if (nc == '\\') {
      output[pos++] = '\\';
    } else if (nc == 'x') {
      if (i + 2 >= colFam.length()) {
        throw new LocalityGroupConfigurationError("Incomplete hex escape sequence in " + colFam);
      }
      output[pos++] = (byte) (0xff & Integer.parseInt(colFam.substring(i + 1, i + 3), 16));
      i += 2;
    } else {
      throw new LocalityGroupConfigurationError("Expected 'x' or '\\' after '\\' in " + colFam);
    }
    return pos;
  }

  public static String encodeColumnFamilies(Set<Text> colFams) {
    SortedSet<String> ecfs = new TreeSet<>();
    StringBuilder sb = new StringBuilder();
    for (Text text : colFams) {
      String ecf = encodeColumnFamily(sb, text.getBytes(), text.getLength());
      ecfs.add(ecf);
    }
    return Joiner.on(",").join(ecfs);
  }

  public static String encodeColumnFamily(ByteSequence bs) {
    if (bs.offset() != 0) {
      throw new IllegalArgumentException("The offset cannot be non-zero.");
    }
    return encodeColumnFamily(new StringBuilder(), bs.getBackingArray(), bs.length());
  }

  private static String encodeColumnFamily(StringBuilder sb, byte[] ba, int len) {
    sb.setLength(0);
    for (int i = 0; i < len; i++) {
      int c = 0xff & ba[i];
      if (c == '\\') {
        sb.append("\\\\");
      } else if (c >= 32 && c <= 126 && c != ',') {
        sb.append((char) c);
      } else {
        sb.append("\\x").append(String.format("%02X", c));
      }
    }
    return sb.toString();
  }

  public static class PartitionedMutation extends Mutation {
    private final byte[] row;
    private final List<ColumnUpdate> updates;

    public PartitionedMutation(byte[] row, List<ColumnUpdate> updates) {
      this.row = row;
      this.updates = updates;
    }

    @Override
    public byte[] getRow() {
      return row;
    }

    @Override
    public List<ColumnUpdate> getUpdates() {
      return updates;
    }

    @Override
    public TMutation toThrift() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int hashCode() {
      throw new UnsupportedOperationException();
    }

    @Override
    @SuppressFBWarnings(value = "EQ_UNUSUAL",
        justification = "method expected to be unused or overridden")
    public boolean equals(Object o) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Mutation m) {
      throw new UnsupportedOperationException();
    }
  }

  public static class Partitioner {

    private final Map<ByteSequence,Integer> colfamToLgidMap;
    private final PreAllocatedArray<Map<ByteSequence,MutableLong>> groups;

    public Partitioner(PreAllocatedArray<Map<ByteSequence,MutableLong>> groups) {
      this.groups = groups;
      this.colfamToLgidMap = new HashMap<>();

      for (int i = 0; i < groups.length; i++) {
        for (ByteSequence cf : groups.get(i).keySet()) {
          colfamToLgidMap.put(cf, i);
        }
      }
    }

    public void partition(List<Mutation> mutations,
        PreAllocatedArray<List<Mutation>> partitionedMutations) {

      MutableByteSequence mbs = new MutableByteSequence(new byte[0], 0, 0);

      PreAllocatedArray<List<ColumnUpdate>> parts = new PreAllocatedArray<>(groups.length + 1);

      for (Mutation mutation : mutations) {
        if (mutation.getUpdates().size() == 1) {
          int lgid = getLgid(mbs, mutation.getUpdates().get(0));
          partitionedMutations.get(lgid).add(mutation);
        } else {
          partitionMultipleUpdates(mutation, partitionedMutations, parts, mbs);
        }
      }
    }

    private void partitionMultipleUpdates(Mutation mutation,
        PreAllocatedArray<List<Mutation>> partitionedMutations,
        PreAllocatedArray<List<ColumnUpdate>> parts, MutableByteSequence mbs) {
      for (int i = 0; i < parts.length; i++) {
        parts.set(i, null);
      }

      int lgcount = 0;

      for (ColumnUpdate cu : mutation.getUpdates()) {
        int lgid = getLgid(mbs, cu);

        if (parts.get(lgid) == null) {
          parts.set(lgid, new ArrayList<>());
          lgcount++;
        }

        parts.get(lgid).add(cu);
      }

      if (lgcount == 1) {
        addSingleGroup(mutation, partitionedMutations, parts);
      } else {
        addMultipleGroups(mutation, partitionedMutations, parts);
      }
    }

    private void addSingleGroup(Mutation mutation,
        PreAllocatedArray<List<Mutation>> partitionedMutations,
        PreAllocatedArray<List<ColumnUpdate>> parts) {
      for (int i = 0; i < parts.length; i++) {
        if (parts.get(i) != null) {
          partitionedMutations.get(i).add(mutation);
          break;
        }
      }
    }

    private void addMultipleGroups(Mutation mutation,
        PreAllocatedArray<List<Mutation>> partitionedMutations,
        PreAllocatedArray<List<ColumnUpdate>> parts) {
      for (int i = 0; i < parts.length; i++) {
        if (parts.get(i) != null) {
          partitionedMutations.get(i).add(new PartitionedMutation(mutation.getRow(), parts.get(i)));
        }
      }
    }

    private Integer getLgid(MutableByteSequence mbs, ColumnUpdate cu) {
      mbs.setArray(cu.getColumnFamily(), 0, cu.getColumnFamily().length);
      Integer lgid = colfamToLgidMap.get(mbs);
      if (lgid == null) {
        lgid = groups.length;
      }
      return lgid;
    }
  }

  public static void seek(FileSKVIterator reader, Range range, String lgName,
      Map<String,ArrayList<ByteSequence>> localityGroupCF) throws IOException {

    Collection<ByteSequence> families;
    boolean inclusive;
    if (lgName == null) {
      Set<ByteSequence> nonDefaultFamilies = new HashSet<>();
      localityGroupCF.forEach((k, v) -> {
        if (k != null) {
          nonDefaultFamilies.addAll(v);
        }
      });

      families = nonDefaultFamilies;
      inclusive = false;
    } else {
      families = localityGroupCF.get(lgName);
      inclusive = true;
    }

    reader.seek(range, families, inclusive);
  }

  public static void ensureNonOverlappingGroups(Map<String,Set<Text>> groups) {
    HashSet<Text> all = new HashSet<>();
    for (Entry<String,Set<Text>> entry : groups.entrySet()) {
      validateGroup(entry, all);
      all.addAll(entry.getValue());
    }
  }

  private static void validateGroup(Entry<String,Set<Text>> entry, HashSet<Text> all) {
    if (!Collections.disjoint(all, entry.getValue())) {
      throw new IllegalArgumentException(
          "Group " + entry.getKey() + " overlaps with another group");
    }

    if (entry.getValue().isEmpty()) {
      throw new IllegalArgumentException("Group " + entry.getKey() + " is empty");
    }
  }
}
