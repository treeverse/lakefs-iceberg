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

public class LakeFSInputFile implements InputFile {

    private final String location;
    private final FileSystem fs;
    private final Path path;
    private final Configuration conf;
    private FileStatus stat = null;
    private Long length = null;
    private NativeFileCryptoParameters nativeDecryptionParameters;

    private final HadoopInputFile hadoopInputFile;
    
    public LakeFSInputFile(HadoopInputFile hadoopInputFile) {
        this.hadoopInputFile = hadoopInputFile;
    }

    public Configuration conf() {
        return hadoopConf.get();
    }

    @Override
    public String location() {
        String location = this.location;
        location = StringUtils.substringAfter(location, "//");
        location = StringUtils.substringAfter(location, "/");
        location = StringUtils.substringAfter(location, "/");
        return location;
    }
}
