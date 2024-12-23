
package org.apache.accumulo.core.file.rfile;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.cli.ConfigOpts;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.crypto.CryptoFactoryLoader;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.blockfile.impl.CachableBlockFile.CachableBuilder;
import org.apache.accumulo.core.file.rfile.RFile.Reader;
import org.apache.accumulo.core.file.rfile.RFile.Writer;
import org.apache.accumulo.core.file.rfile.bcfile.BCFile;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.accumulo.start.spi.KeywordExecutable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.beust.jcommander.Parameter;
import com.google.auto.service.AutoService;

/**
 * Split an RFile into large and small key/value files.
 */
@AutoService(KeywordExecutable.class)
public class SplitLarge implements KeywordExecutable {

  static class Opts extends ConfigOpts {
    @Parameter(names = "-m",
        description = "the maximum size of the key/value pair to shunt to the small file")
    long maxSize = 10 * 1024 * 1024;
    @Parameter(description = "<file.rf> { <file.rf> ... }")
    List<String> files = new ArrayList<>();
  }

  public static void main(String[] args) throws Exception {
    new SplitLarge().execute(args);
  }

  @Override
  public String keyword() {
    return "split-large";
  }

  @Override
  public String description() {
    return "Splits an RFile into large and small key/value files";
  }

  @Override
  public void execute(String[] args) throws Exception {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    Opts opts = new Opts();
    opts.parseArgs("accumulo split-large", args);

    for (String file : opts.files) {
      processFile(conf, fs, opts, file);
    }
  }

  private void processFile(Configuration conf, FileSystem fs, Opts opts, String file)
      throws Exception {
    AccumuloConfiguration aconf = opts.getSiteConfiguration();
    CryptoService cs = CryptoFactoryLoader.getServiceForServer(aconf);
    Path path = new Path(file);
    CachableBuilder cb = new CachableBuilder().fsPath(fs, path).conf(conf).cryptoService(cs);
    try (Reader iter = new RFile.Reader(cb)) {
      validateFileName(file);
      String smallName = getSmallFileName(file);
      String largeName = getLargeFileName(file);
      splitFile(fs, conf, cs, aconf, iter, smallName, largeName, opts.maxSize);
    }
  }

  private void validateFileName(String file) {
    if (!file.endsWith(".rf")) {
      throw new IllegalArgumentException("File must end with .rf");
    }
  }

  private String getSmallFileName(String file) {
    return file.substring(0, file.length() - 3) + "_small.rf";
  }

  private String getLargeFileName(String file) {
    return file.substring(0, file.length() - 3) + "_large.rf";
  }

  private void splitFile(FileSystem fs, Configuration conf, CryptoService cs,
      AccumuloConfiguration aconf, Reader iter, String smallName, String largeName, long maxSize)
      throws Exception {
    int blockSize = (int) aconf.getAsBytes(Property.TABLE_FILE_BLOCK_SIZE);
    try (
        Writer small = new RFile.Writer(
            new BCFile.Writer(fs.create(new Path(smallName)), null, "gz", conf, cs), blockSize);
        Writer large = new RFile.Writer(
            new BCFile.Writer(fs.create(new Path(largeName)), null, "gz", conf, cs), blockSize)) {

      small.startDefaultLocalityGroup();
      large.startDefaultLocalityGroup();
      iter.seek(new Range(), new ArrayList<>(), false);
      while (iter.hasTop()) {
        Key key = iter.getTopKey();
        Value value = iter.getTopValue();
        if (key.getSize() + value.getSize() < maxSize) {
          small.append(key, value);
        } else {
          large.append(key, value);
        }
        iter.next();
      }
    }
  }
}
