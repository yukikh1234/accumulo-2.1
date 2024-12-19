
package org.apache.accumulo.core.util.format;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

/**
 * Does not show contents from scan, only displays statistics. Beware that this work is being done
 * client side and this was developed as a utility for debugging. If used on large result sets it
 * will likely fail.
 */
public class StatisticsDisplayFormatter extends AggregatingFormatter {
  private Map<String,Long> classifications = new HashMap<>();
  private Map<String,Long> columnFamilies = new HashMap<>();
  private Map<String,Long> columnQualifiers = new HashMap<>();
  private long total = 0;

  @Override
  protected void aggregateStats(Entry<Key,Value> entry) {
    incrementCount(classifications, entry.getKey().getColumnVisibility().toString());
    incrementCount(columnFamilies, entry.getKey().getColumnFamily().toString());
    incrementCount(columnQualifiers, entry.getKey().getColumnQualifier().toString());
    ++total;
  }

  private void incrementCount(Map<String,Long> map, String key) {
    Long count = map.get(key);
    map.put(key, count != null ? count + 1 : 0L);
  }

  @Override
  protected String getStats() {
    StringBuilder buf = new StringBuilder();
    appendSection(buf, "CLASSIFICATIONS", classifications);
    appendSection(buf, "COLUMN FAMILIES", columnFamilies);
    appendSection(buf, "COLUMN QUALIFIERS", columnQualifiers);

    buf.append(total).append(" entries matched.");
    resetStats();

    return buf.toString();
  }

  private void appendSection(StringBuilder buf, String title, Map<String,Long> map) {
    buf.append(title).append(":\n");
    buf.append("-".repeat(title.length())).append("\n");
    for (Map.Entry<String,Long> entry : map.entrySet()) {
      buf.append("\t").append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
    }
  }

  private void resetStats() {
    total = 0;
    classifications.clear();
    columnFamilies.clear();
    columnQualifiers.clear();
  }
}
