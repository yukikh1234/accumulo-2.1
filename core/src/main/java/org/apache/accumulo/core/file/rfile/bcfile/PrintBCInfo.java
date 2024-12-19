
package org.apache.accumulo.core.file.rfile.bcfile;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.cli.ConfigOpts;
import org.apache.accumulo.core.conf.SiteConfiguration;
import org.apache.accumulo.core.file.rfile.bcfile.BCFile.MetaIndexEntry;
import org.apache.accumulo.core.spi.crypto.CryptoService;
import org.apache.accumulo.core.spi.crypto.NoCryptoServiceFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.beust.jcommander.Parameter;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings(value = "DM_EXIT",
    justification = "System.exit is fine here because it's a utility class executed by a main()")
public class PrintBCInfo {
  SiteConfiguration siteConfig;
  Configuration conf;
  FileSystem fs;
  Path path;
  CryptoService cryptoService = NoCryptoServiceFactory.NONE;

  public void printMetaBlockInfo() throws IOException {
    try (FSDataInputStream fsin = openInputStream(); BCFile.Reader bcfr =
        new BCFile.Reader(fsin, fs.getFileStatus(path).getLen(), conf, cryptoService)) {
      printMetaBlockDetails(bcfr);
    }
  }

  private FSDataInputStream openInputStream() throws IOException {
    return fs.open(path);
  }

  private void printMetaBlockDetails(BCFile.Reader bcfr) {
    Set<Entry<String,MetaIndexEntry>> es = bcfr.metaIndex.index.entrySet();
    PrintStream out = System.out;

    for (Entry<String,MetaIndexEntry> entry : es) {
      out.println("Meta block     : " + entry.getKey());
      out.println("      Raw size             : "
          + String.format("%,d", entry.getValue().getRegion().getRawSize()) + " bytes");
      out.println("      Compressed size      : "
          + String.format("%,d", entry.getValue().getRegion().getCompressedSize()) + " bytes");
      out.println(
          "      Compression type     : " + entry.getValue().getCompressionAlgorithm().getName());
      out.println();
    }
  }

  static class Opts extends ConfigOpts {
    @Parameter(description = " <file>")
    String file;
  }

  public PrintBCInfo(String[] args) throws Exception {
    initialize(args);
  }

  private void initialize(String[] args) throws Exception {
    Opts opts = new Opts();
    opts.parseArgs("PrintInfo", args);

    validateFile(opts);

    siteConfig = opts.getSiteConfiguration();
    conf = new Configuration();
    path = new Path(opts.file);
    fs = determineFileSystem();
  }

  private void validateFile(Opts opts) {
    if (opts.file.isEmpty()) {
      System.err.println("No files were given");
      System.exit(-1);
    }
  }

  private FileSystem determineFileSystem() throws IOException {
    FileSystem hadoopFs = FileSystem.get(conf);
    FileSystem localFs = FileSystem.getLocal(conf);
    if (path.toString().contains(":")) {
      return path.getFileSystem(conf);
    } else {
      return hadoopFs.exists(path) ? hadoopFs : localFs;
    }
  }

  public CryptoService getCryptoService() {
    return cryptoService;
  }

  public void setCryptoService(CryptoService cryptoService) {
    this.cryptoService = cryptoService;
  }
}
