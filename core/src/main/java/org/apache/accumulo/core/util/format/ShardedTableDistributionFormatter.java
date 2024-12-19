
package org.apache.accumulo.core.util.format;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

/**
 * Formats the rows in a METADATA table scan to show distribution of shards over servers per day.
 * This can be used to determine the effectiveness of the ShardedTableLoadBalancer
 *
 * Use this formatter with the following scan command in the shell:
 *
 * scan -b tableId -c ~tab:loc
 */
public class ShardedTableDistributionFormatter extends AggregatingFormatter {

  private final Map<String,HashSet<String>> countsByDay = new HashMap<>();

  @Override
  protected void aggregateStats(Entry<Key,Value> entry) {
    if (isRelevantEntry(entry)) {
      String day = parseDay(entry.getKey().getRow().toString());
      String server = entry.getValue().toString();
      addServerToDay(day, server);
    }
  }

  private boolean isRelevantEntry(Entry<Key,Value> entry) {
    return entry.getKey().getColumnFamily().toString().equals("~tab")
        && entry.getKey().getColumnQualifier().toString().equals("loc");
  }

  private String parseDay(String row) {
    int semicolon = row.indexOf(";");
    if (semicolon == -1) {
      return "NULL    ";
    } else {
      return row.substring(semicolon + 1, semicolon + 9);
    }
  }

  private void addServerToDay(String day, String server) {
    countsByDay.computeIfAbsent(day, k -> new HashSet<>()).add(server);
  }

  @Override
  protected String getStats() {
    StringBuilder buf = new StringBuilder();
    buf.append("DAY   \t\tSERVERS\n");
    buf.append("------\t\t-------\n");
    for (Map.Entry<String,HashSet<String>> entry : countsByDay.entrySet()) {
      buf.append(entry.getKey()).append("\t\t").append(entry.getValue().size()).append("\n");
    }
    return buf.toString();
  }
}
