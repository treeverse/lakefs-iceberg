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

public class LakeFSFileIO extends HadoopFileIO {

    String lakeFSRepo = "example-repo";
    String lakeFSBranch = "branch-a";

    private SerializableSupplier<Configuration> hadoopConf;
    private SerializableMap<String, String> properties = SerializableMap.copyOf(ImmutableMap.of());


    public LakeFSFileIO() {}

    public LakeFSFileIO(Configuration hadoopConf) {
        this(new SerializableConfiguration(hadoopConf)::get);
    }

    public LakeFSFileIO(SerializableSupplier<Configuration> hadoopConf) {
        this.hadoopConf = hadoopConf;
    }

    public Configuration conf() {
        return hadoopConf.get();
    }

    @Override
    public void initialize(Map<String, String> props) {
        this.properties = SerializableMap.copyOf(props);
    }

    @Override
    public InputFile newInputFile(String path) {
        if (!path.startsWith("s3a://")){
            path = "s3a://" + lakeFSRepo + "/" + lakeFSBranch + "/" + path;
        }
        return new LakeFSInputFile(HadoopInputFile.fromLocation(path, hadoopConf.get()));
    }

    @Override
    public InputFile newInputFile(String path, long length) {
        if (!path.startsWith("s3a://")){
            path = "s3a://" + lakeFSRepo + "/" + lakeFSBranch + "/" + path;
        }
        return new LakeFSInputFile(HadoopInputFile.fromLocation(path, length, hadoopConf.get());
    }

    @Override
    public OutputFile newOutputFile(String path) {
        String newPath = path;
        if (!path.startsWith("s3a://")){
            path = "s3a://" + lakeFSRepo + "/" + lakeFSBranch + "/" + path;
        }
        return new LakeFSOutputFile(HadoopOutputFile.fromPath(new Path(path), hadoopConf.get()));
    }
    
//    public void setLakeFSRepo(String lakeFSRepo){
//        this.lakeFSRepo = lakeFSRepo;
//    }
//
//    public void setLakeFSBranch(String lakeFSBranch){
//        this.lakeFSBranch = lakeFSBranch;
//    }
}
