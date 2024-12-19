
package org.apache.accumulo.core.file.rfile;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.PrintStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

import com.google.common.util.concurrent.AtomicLongMap;

public class VisMetricsGatherer
    implements MetricsGatherer<Map<String,ArrayList<VisibilityMetric>>> {
  private static final String KEY_HASH_ALGORITHM = "SHA-256";

  protected Map<String,AtomicLongMap<String>> metric;
  protected Map<String,AtomicLongMap<String>> blocks;
  protected ArrayList<Long> numEntries;
  protected ArrayList<Integer> numBlocks;
  private final ArrayList<String> inBlock;
  protected ArrayList<String> localityGroups;
  private int numLG;
  private Map<String,ArrayList<ByteSequence>> localityGroupCF;

  public VisMetricsGatherer() {
    metric = new HashMap<>();
    blocks = new HashMap<>();
    numEntries = new ArrayList<>();
    numBlocks = new ArrayList<>();
    inBlock = new ArrayList<>();
    localityGroups = new ArrayList<>();
    numLG = 0;
  }

  @Override
  public void init(Map<String,ArrayList<ByteSequence>> cf) {
    localityGroupCF = cf;
  }

  @Override
  public void startLocalityGroup(Text oneCF) {
    String name = determineLocalityGroupName(oneCF);
    localityGroups.add(name);
    metric.put(name, AtomicLongMap.create(new HashMap<>()));
    blocks.put(name, AtomicLongMap.create(new HashMap<>()));
    numLG++;
    numEntries.add((long) 0);
    numBlocks.add(0);
  }

  private String determineLocalityGroupName(Text oneCF) {
    ByteSequence cf = new ArrayByteSequence(oneCF.toString());
    for (Entry<String,ArrayList<ByteSequence>> entry : localityGroupCF.entrySet()) {
      if (entry.getValue().contains(cf)) {
        return entry.getKey();
      }
    }
    return null;
  }

  @Override
  public void addMetric(Key key, Value val) {
    String myMetric = key.getColumnVisibility().toString();
    String currLG = localityGroups.get(numLG - 1);
    incrementMetric(currLG, myMetric);
    numEntries.set(numLG - 1, numEntries.get(numLG - 1) + 1);
    updateBlocks(currLG, myMetric);
  }

  private void incrementMetric(String currLG, String myMetric) {
    AtomicLongMap<String> lgMetrics = metric.get(currLG);
    lgMetrics.getAndIncrement(myMetric);
  }

  private void updateBlocks(String currLG, String myMetric) {
    if (!inBlock.contains(myMetric)) {
      AtomicLongMap<String> lgBlocks = blocks.get(currLG);
      lgBlocks.getAndIncrement(myMetric);
      inBlock.add(myMetric);
    }
  }

  @Override
  public void startBlock() {
    inBlock.clear();
    numBlocks.set(numLG - 1, numBlocks.get(numLG - 1) + 1);
  }

  @Override
  public void printMetrics(boolean hash, String metricWord, PrintStream out) {
    for (int i = 0; i < numLG; i++) {
      printLocalityGroupMetrics(hash, metricWord, out, i);
    }
  }

  private void printLocalityGroupMetrics(boolean hash, String metricWord, PrintStream out, int i) {
    String lGName = localityGroups.get(i);
    out.print("Locality Group: ");
    out.println(lGName == null ? "<DEFAULT>" : lGName);
    out.printf("%-27s", metricWord);
    out.println("Number of keys\t   Percent of keys\tNumber of blocks\tPercent of blocks");

    for (Entry<String,Long> entry : metric.get(lGName).asMap().entrySet()) {
      printMetricDetails(hash, out, i, entry, lGName);
    }

    out.println("Number of keys: " + numEntries.get(i));
    out.println();
  }

  private void printMetricDetails(boolean hash, PrintStream out, int i, Entry<String,Long> entry,
      String lGName) {
    String metricKey = hash ? hashKey(entry.getKey()) : entry.getKey();
    out.printf("%-20s", metricKey);
    out.printf("\t\t%d\t\t\t%.2f%%\t\t\t%d\t\t   %.2f%%\n", entry.getValue(),
        calculatePercentage(entry.getValue(), numEntries.get(i)),
        blocks.get(lGName).get(entry.getKey()),
        calculatePercentage(blocks.get(lGName).get(entry.getKey()), numBlocks.get(i)));
  }

  private String hashKey(String key) {
    try {
      byte[] encodedBytes =
          MessageDigest.getInstance(KEY_HASH_ALGORITHM).digest(key.getBytes(UTF_8));
      return new String(encodedBytes, UTF_8).substring(0, 8);
    } catch (NoSuchAlgorithmException e) {
      return "HASH_ERR";
    }
  }

  private double calculatePercentage(long part, long total) {
    return ((double) part / total) * 100;
  }

  @Override
  public Map<String,ArrayList<VisibilityMetric>> getMetrics() {
    Map<String,ArrayList<VisibilityMetric>> getMetrics = new HashMap<>();
    for (int i = 0; i < numLG; i++) {
      addVisibilityMetrics(getMetrics, i);
    }
    return getMetrics;
  }

  private void addVisibilityMetrics(Map<String,ArrayList<VisibilityMetric>> getMetrics, int i) {
    String lGName = localityGroups.get(i);
    ArrayList<VisibilityMetric> rows = new ArrayList<>();
    for (Entry<String,Long> entry : metric.get(lGName).asMap().entrySet()) {
      rows.add(createVisibilityMetric(entry, i, lGName));
    }
    getMetrics.put(lGName, rows);
  }

  private VisibilityMetric createVisibilityMetric(Entry<String,Long> entry, int i, String lGName) {
    long vis = entry.getValue();
    double visPer = calculatePercentage(vis, numEntries.get(i));
    long blocksIn = blocks.get(lGName).get(entry.getKey());
    double blocksPer = calculatePercentage(blocksIn, numBlocks.get(i));
    return new VisibilityMetric(entry.getKey(), vis, visPer, blocksIn, blocksPer);
  }
}
