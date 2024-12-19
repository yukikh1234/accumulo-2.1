
package org.apache.accumulo.core.file;

import java.io.IOException;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.file.map.MapFileOperations;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.accumulo.core.file.rfile.RFileOperations;
import org.apache.accumulo.core.summary.SummaryWriter;
import org.apache.hadoop.fs.Path;

class DispatchingFileFactory extends FileOperations {

  private FileOperations findFileFactory(FileOptions options) {
    String file = options.getFilename();
    Path p = new Path(file);
    String name = p.getName();
    if (name.startsWith(Constants.MAPFILE_EXTENSION + "_")) {
      return new MapFileOperations();
    }
    String extension = extractFileExtension(name);
    return determineFileOperations(extension);
  }

  private String extractFileExtension(String name) {
    String[] sp = name.split("\\.");
    if (sp.length < 2) {
      throw new IllegalArgumentException("File name " + name + " has no extension");
    }
    return sp[sp.length - 1];
  }

  private FileOperations determineFileOperations(String extension) {
    if (extension.equals(Constants.MAPFILE_EXTENSION)
        || extension.equals(Constants.MAPFILE_EXTENSION + "_tmp")) {
      return new MapFileOperations();
    } else if (extension.equals(RFile.EXTENSION) || extension.equals(RFile.EXTENSION + "_tmp")) {
      return new RFileOperations();
    } else {
      throw new IllegalArgumentException("File type " + extension + " not supported");
    }
  }

  @Override
  protected long getFileSize(FileOptions options) throws IOException {
    return findFileFactory(options).getFileSize(options);
  }

  @Override
  protected FileSKVWriter openWriter(FileOptions options) throws IOException {
    FileOperations fileOps = new RFileOperations();
    FileSKVWriter writer = fileOps.openWriter(options);
    writer = applyBloomFilterLayer(writer, options);
    return SummaryWriter.wrap(writer, options.getTableConfiguration(),
        options.isAccumuloStartEnabled());
  }

  private FileSKVWriter applyBloomFilterLayer(FileSKVWriter writer, FileOptions options) {
    if (options.getTableConfiguration().getBoolean(Property.TABLE_BLOOM_ENABLED)) {
      return new BloomFilterLayer.Writer(writer, options.getTableConfiguration(),
          options.isAccumuloStartEnabled());
    }
    return writer;
  }

  @Override
  protected FileSKVIterator openIndex(FileOptions options) throws IOException {
    return findFileFactory(options).openIndex(options);
  }

  @Override
  protected FileSKVIterator openReader(FileOptions options) throws IOException {
    FileSKVIterator iter = findFileFactory(options).openReader(options);
    return applyBloomFilterReader(iter, options);
  }

  private FileSKVIterator applyBloomFilterReader(FileSKVIterator iter, FileOptions options) {
    if (options.getTableConfiguration().getBoolean(Property.TABLE_BLOOM_ENABLED)) {
      return new BloomFilterLayer.Reader(iter, options.getTableConfiguration());
    }
    return iter;
  }

  @Override
  protected FileSKVIterator openScanReader(FileOptions options) throws IOException {
    return findFileFactory(options).openScanReader(options);
  }
}
