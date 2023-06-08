package io.lakefs.iceberg;

// TODO lynn: Go over import list
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.BulkDeletionFailureException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SupportsBulkOperations;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.util.SerializableMap;
import org.apache.iceberg.util.SerializableSupplier;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LakeFSOutputFile implements OutputFile {

    private final FileSystem fs;
    private final Path path;
    private final Configuration conf;
    private NativeFileCryptoParameters nativeEncryptionParameters;

    private final HadoopOutputFile hadoopOutputFile;
    
    public LakeFSOutputFile(HadoopOutputFile hadoopOutputFile) {

        this.hadoopOutputFile = hadoopOutputFile;
    }

    public static OutputFile fromLocation(CharSequence location, Configuration conf) {
        return hadoopOutputFile.fromLocation(location, conf);
    }

    public static OutputFile fromLocation(CharSequence location, FileSystem fs) {
        return hadoopOutputFile.fromLocation(location, fs);
    }

    public static OutputFile fromPath(Path path, Configuration conf) {
        return hadoopOutputFile.fromPath(path, conf);
    }

    public static OutputFile fromPath(Path path, FileSystem fs) {
        return hadoopOutputFile.fromPath(path, fs);
    }

    public static OutputFile fromPath(Path path, FileSystem fs, Configuration conf) {
        return hadoopOutputFile.fromPath(path, fs, conf);
    }

    @Override
    public PositionOutputStream create() {
        return hadoopOutputFile.create();
    }

    @Override
    public PositionOutputStream createOrOverwrite() {
        return hadoopOutputFile.createOrOverwrite();
    }

    public Path getPath() {
        return hadoopOutputFile.getPath();
    }

    public Configuration getConf()
        return hadoopOutputFile.getConf();
    }

    public FileSystem getFileSystem() {
        return hadoopOutputFile.getFileSystem();
    }

    public String location() {
        String location = path.toString();
        location = StringUtils.substringAfter(location, "//");
        location = StringUtils.substringAfter(location, "/");
        location = StringUtils.substringAfter(location, "/");
        return location;
    }

    // TODO lynn: figure out
//    public InputFile toInputFile() {
//        return HadoopInputFile.fromPath(path, fs, conf);
//    }

    public String toString() {
        return location();
    }

    public NativeFileCryptoParameters nativeCryptoParameters() {
        return hadoopOutputFile.nativeCryptoParameters();
    }


    public void setNativeCryptoParameters(NativeFileCryptoParameters nativeCryptoParameters) {
        return hadoopOutputFile.setNativeCryptoParameters(nativeCryptoParameters);
    }


    public Configuration conf() {

        return hadoopOutputFile.conf();
    }
}
