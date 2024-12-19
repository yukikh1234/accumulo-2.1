
package org.apache.accumulo.core.file.rfile;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toCollection;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.cli.ConfigOpts;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.crypto.CryptoFactoryLoader;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.FileOperations;
import org.apache.accumulo.core.file.FileSKVIterator;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.MultiIterator;
import org.apache.accumulo.core.spi.crypto.CryptoEnvironment;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.accumulo.core.util.TextUtil;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.datasketches.quantiles.ItemsSketch;
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria;
import org.apache.datasketches.quantilescommon.QuantilesUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.google.auto.service.AutoService;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@AutoService(KeywordExecutable.class)
@SuppressFBWarnings(value = "PATH_TRAVERSAL_OUT",
    justification = "app is run in same security context as user providing the filename")
public class GenerateSplits implements KeywordExecutable {
  private static final Logger log = LoggerFactory.getLogger(GenerateSplits.class);

  private static final Set<Character> allowedChars = new HashSet<>();

  private static final String encodeFlag = "-b64";

  static class Opts extends ConfigOpts {
    @Parameter(names = {"-n", "--num"},
        description = "The number of split points to generate. Can be used to create n+1 tablets. Cannot use with the split size option.")
    public int numSplits = 0;

    @Parameter(names = {"-ss", "--split-size"},
        description = "The minimum split size in uncompressed bytes. Cannot use with num splits option.")
    public long splitSize = 0;

    @Parameter(names = {encodeFlag, "--base64encoded"},
        description = "Base 64 encode the split points")
    public boolean base64encode = false;

    @Parameter(names = {"-sf", "--splits-file"}, description = "Output the splits to a file")
    public String outputFile;

    @Parameter(description = "<file|directory>[ <file|directory>...] -n <num> | -ss <split_size>")
    public List<String> files = new ArrayList<>();
  }

  @Override
  public String keyword() {
    return "generate-splits";
  }

  @Override
  public String description() {
    return "Generate split points from a set of 1 or more rfiles";
  }

  public static void main(String[] args) throws Exception {
    new GenerateSplits().execute(args);
  }

  @Override
  public void execute(String[] args) throws Exception {
    Opts opts = new Opts();
    opts.parseArgs(GenerateSplits.class.getName(), args);
    validateOptions(opts);

    Configuration hadoopConf = new Configuration();
    SiteConfiguration siteConf = opts.getSiteConfiguration();
    CryptoService cryptoService = CryptoFactoryLoader
        .getServiceForClient(CryptoEnvironment.Scope.TABLE, siteConf.getAllCryptoProperties());

    List<Path> filePaths = resolveFilePaths(opts, hadoopConf);
    boolean encode = opts.base64encode;
    TreeSet<String> splits =
        calculateSplits(opts, siteConf, hadoopConf, filePaths, encode, cryptoService);
    writeSplitsToFile(opts, splits);
  }

  private void validateOptions(Opts opts) {
    if (opts.files.isEmpty()) {
      throw new IllegalArgumentException("No files were given");
    }
    if (opts.numSplits > 0 && opts.splitSize > 0) {
      throw new IllegalArgumentException("Requested number of splits and split size.");
    }
    if (opts.numSplits == 0 && opts.splitSize == 0) {
      throw new IllegalArgumentException("Required number of splits or split size.");
    }
  }

  private List<Path> resolveFilePaths(Opts opts, Configuration hadoopConf) throws IOException {
    FileSystem fs = FileSystem.get(hadoopConf);
    List<Path> filePaths = new ArrayList<>();
    for (String file : opts.files) {
      Path path = new Path(file);
      fs = PrintInfo.resolveFS(log, hadoopConf, path);
      filePaths.addAll(getFiles(fs, path));
    }
    if (filePaths.isEmpty()) {
      throw new IllegalArgumentException("No files were found in " + opts.files);
    } else {
      log.trace("Found the following files: {}", filePaths);
    }
    return filePaths;
  }

  private TreeSet<String> calculateSplits(Opts opts, SiteConfiguration siteConf,
      Configuration hadoopConf, List<Path> filePaths, boolean encode, CryptoService cryptoService)
      throws IOException {
    TreeSet<String> splits;
    if (opts.splitSize == 0) {
      splits = getIndexKeys(siteConf, hadoopConf, filePaths, opts.numSplits, encode, cryptoService);
      if (splits.size() < opts.numSplits) {
        log.info("Only found {} indexed keys but need {}. Doing a full scan on files {}",
            splits.size(), opts.numSplits, filePaths);
        splits = getSplitsFromFullScan(siteConf, hadoopConf, filePaths, opts.numSplits, encode,
            cryptoService);
      }
    } else {
      splits =
          getSplitsBySize(siteConf, hadoopConf, filePaths, opts.splitSize, encode, cryptoService);
    }
    return getDesiredSplits(splits, opts.numSplits);
  }

  private TreeSet<String> getDesiredSplits(TreeSet<String> splits, int requestedNumSplits) {
    int numFound = splits.size();
    TreeSet<String> desiredSplits;
    if (numFound > requestedNumSplits) {
      desiredSplits = getEvenlySpacedSplits(numFound, requestedNumSplits, splits.iterator());
    } else {
      if (numFound < requestedNumSplits) {
        log.warn("Only found {} splits", numFound);
      }
      desiredSplits = splits;
    }
    log.info("Generated {} splits", desiredSplits.size());
    return desiredSplits;
  }

  private void writeSplitsToFile(Opts opts, TreeSet<String> desiredSplits) throws IOException {
    if (opts.outputFile != null) {
      log.info("Writing splits to file {} ", opts.outputFile);
      try (var writer = new PrintWriter(new BufferedWriter(
          new OutputStreamWriter(new FileOutputStream(opts.outputFile), UTF_8)))) {
        desiredSplits.forEach(writer::println);
      }
    } else {
      desiredSplits.forEach(System.out::println);
    }
  }

  private List<Path> getFiles(FileSystem fs, Path path) throws IOException {
    List<Path> filePaths = new ArrayList<>();
    if (fs.getFileStatus(path).isDirectory()) {
      var iter = fs.listFiles(path, true);
      while (iter.hasNext()) {
        filePaths.addAll(getFiles(fs, iter.next().getPath()));
      }
    } else {
      if (!path.toString().endsWith(".rf")) {
        throw new IllegalArgumentException("Provided file (" + path + ") does not end with '.rf'");
      }
      filePaths.add(path);
    }
    return filePaths;
  }

  private Text[] getQuantiles(SortedKeyValueIterator<Key,Value> iterator, int numSplits)
      throws IOException {
    var itemsSketch = ItemsSketch.getInstance(Text.class, BinaryComparable::compareTo);
    while (iterator.hasTop()) {
      Text row = iterator.getTopKey().getRow();
      itemsSketch.update(row);
      iterator.next();
    }
    double[] ranks = QuantilesUtil.equallyWeightedRanks(numSplits + 1);
    Text[] items = itemsSketch.getQuantiles(ranks, QuantileSearchCriteria.EXCLUSIVE);
    return Arrays.copyOfRange(items, 1, items.length - 1);
  }

  static TreeSet<String> getEvenlySpacedSplits(int numFound, long requestedNumSplits,
      Iterator<String> splitsIter) {
    TreeSet<String> desiredSplits = new TreeSet<>();
    double increment = (requestedNumSplits + 1.0) / numFound;
    log.debug("Found {} splits but requested {} so picking incrementally by {}", numFound,
        requestedNumSplits, increment);

    double progressToNextSplit = 0;

    for (int i = 0; i < numFound; i++) {
      progressToNextSplit += increment;
      String next = splitsIter.next();
      if (progressToNextSplit > 1 && desiredSplits.size() < requestedNumSplits) {
        desiredSplits.add(next);
        progressToNextSplit -= 1;
      }
    }

    return desiredSplits;
  }

  private static String encode(boolean encode, Text text) {
    if (text == null) {
      return null;
    }
    byte[] bytes = TextUtil.getBytes(text);
    if (encode) {
      return Base64.getEncoder().encodeToString(bytes);
    } else {
      StringBuilder sb = new StringBuilder();
      for (byte aByte : bytes) {
        int c = 0xff & aByte;
        if (allowedChars.contains((char) c)) {
          sb.append((char) c);
        } else {
          throw new UnsupportedOperationException("Non printable char: \\x" + Integer.toHexString(c)
              + " detected. Must use Base64 encoded output.  The behavior around non printable chars changed in 2.1.3 to throw an error, the previous behavior was likely to cause bugs.");
        }
      }
      return sb.toString();
    }
  }

  private TreeSet<String> getIndexKeys(AccumuloConfiguration accumuloConf, Configuration hadoopConf,
      List<Path> files, int requestedNumSplits, boolean base64encode, CryptoService cs)
      throws IOException {
    Text[] splitArray;
    List<SortedKeyValueIterator<Key,Value>> readers = new ArrayList<>(files.size());
    List<FileSKVIterator> fileReaders = new ArrayList<>(files.size());
    try {
      for (Path file : files) {
        FileSKVIterator reader = FileOperations.getInstance().newIndexReaderBuilder()
            .forFile(file.toString(), FileSystem.get(hadoopConf), hadoopConf, cs)
            .withTableConfiguration(accumuloConf).build();
        readers.add(reader);
        fileReaders.add(reader);
      }
      var iterator = new MultiIterator(readers, true);
      splitArray = getQuantiles(iterator, requestedNumSplits);
    } finally {
      for (var r : fileReaders) {
        r.close();
      }
    }

    log.debug("Got {} splits from indices of {}", splitArray.length, files);
    return Arrays.stream(splitArray).map(t -> encode(base64encode, t))
        .collect(toCollection(TreeSet::new));
  }

  private TreeSet<String> getSplitsFromFullScan(SiteConfiguration accumuloConf,
      Configuration hadoopConf, List<Path> files, int numSplits, boolean base64encode,
      CryptoService cs) throws IOException {
    Text[] splitArray;
    List<FileSKVIterator> fileReaders = new ArrayList<>(files.size());
    List<SortedKeyValueIterator<Key,Value>> readers = new ArrayList<>(files.size());
    SortedKeyValueIterator<Key,Value> iterator;

    try {
      for (Path file : files) {
        FileSKVIterator reader = FileOperations.getInstance().newScanReaderBuilder()
            .forFile(file.toString(), FileSystem.get(hadoopConf), hadoopConf, cs)
            .withTableConfiguration(accumuloConf).overRange(new Range(), Set.of(), false).build();
        readers.add(reader);
        fileReaders.add(reader);
      }
      iterator = new MultiIterator(readers, false);
      iterator.seek(new Range(), Collections.emptySet(), false);
      splitArray = getQuantiles(iterator, numSplits);
    } finally {
      for (var r : fileReaders) {
        r.close();
      }
    }

    log.debug("Got {} splits from quantiles across {} files", splitArray.length, files.size());
    return Arrays.stream(splitArray).map(t -> encode(base64encode, t))
        .collect(toCollection(TreeSet::new));
  }

  private TreeSet<String> getSplitsBySize(AccumuloConfiguration accumuloConf,
      Configuration hadoopConf, List<Path> files, long splitSize, boolean base64encode,
      CryptoService cs) throws IOException {
    long currentSplitSize = 0;
    long totalSize = 0;
    TreeSet<String> splits = new TreeSet<>();
    List<FileSKVIterator> fileReaders = new ArrayList<>(files.size());
    List<SortedKeyValueIterator<Key,Value>> readers = new ArrayList<>(files.size());
    SortedKeyValueIterator<Key,Value> iterator;
    try {
      for (Path file : files) {
        FileSKVIterator reader = FileOperations.getInstance().newScanReaderBuilder()
            .forFile(file.toString(), FileSystem.get(hadoopConf), hadoopConf, cs)
            .withTableConfiguration(accumuloConf).overRange(new Range(), Set.of(), false).build();
        readers.add(reader);
        fileReaders.add(reader);
      }
      iterator = new MultiIterator(readers, false);
      iterator.seek(new Range(), Collections.emptySet(), false);
      while (iterator.hasTop()) {
        Key key = iterator.getTopKey();
        Value val = iterator.getTopValue();
        int size = key.getSize() + val.getSize();
        currentSplitSize += size;
        totalSize += size;
        if (currentSplitSize > splitSize) {
          splits.add(encode(base64encode, key.getRow()));
          currentSplitSize = 0;
        }
        iterator.next();
      }
    } finally {
      for (var r : fileReaders) {
        r.close();
      }
    }

    log.debug("Got {} splits with split size {} out of {} total bytes read across {} files",
        splits.size(), splitSize, totalSize, files.size());
    return splits;
  }
}
