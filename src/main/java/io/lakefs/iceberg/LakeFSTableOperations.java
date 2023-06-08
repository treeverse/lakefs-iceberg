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
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

public class LakeFSTableOperations extends HadoopTableOperations {
    private static final Logger LOG = LoggerFactory.getLogger(HadoopTableOperations.class);
    private static final Pattern VERSION_PATTERN = Pattern.compile("v([^\\.]*)\\..*");

    private final Configuration conf;
    private final Path location;
    private final LakeFSFileIO fileIO;
    private final LockManager lockManager;

    private volatile TableMetadata currentMetadata = null;
    private volatile Integer version = null;
    private volatile boolean shouldRefresh = true;

    private String lakeFSRepo = "example-reop";
    private String lakeFSBranch = "branch-a";

    protected LakeFSTableOperations(
            Path location, FileIO fileIO, Configuration conf, LockManager lockManager, String lakeFSRepo, String lakeFSBranch) {
        this.conf = conf;
        this.location = location;
        this.lockManager = lockManager;
//        this.lakeFSRepo = lakeFSRepo;
//        this.lakeFSBranch = lakeFSBranch;
//        fileIO.setLakeFSRepo(lakeFSRepo);
//        fileIO.setLakeFSBranch(lakeFSBranch);
        this.fileIO = fileIO;
    }

    @Override
    public String metadataFileLocation(String fileName) {
        String path = metadataPath(fileName).toString();
        path = StringUtils.substringAfter(path, "//");
        path = StringUtils.substringAfter(path, "/");
        path = StringUtils.substringAfter(path, "/");
        return path;
    }

    @Override
    private Path metadataRoot() {
        String newRootLocation = "s3a://" + this.lakeFSRepo + "/" + this.lakeFSBranch + "/" + location;
        return new Path(newRootLocation, "metadata");
    }
}
