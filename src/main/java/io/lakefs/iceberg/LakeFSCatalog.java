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

public class LakeFSCatalog extends HadoopCatalog {
    private static final Logger LOG = LoggerFactory.getLogger(LakeFSCatalog.class);
    private static final String LAKEFS_SCHEME = "lakefs";
    private String lakeFSRepo;

    @Override
    public void initialize(String name, Map<String, String> properties) {
        String lakefsRepositoryURI = properties.get(CatalogProperties.WAREHOUSE_LOCATION);
        Preconditions.checkArgument(lakefsRepositoryURI.matches("lakefs://[^/]+"),
                "Warehouse path must be a lakeFS repository URI without a path (e.g. lakefs://example-repo)");
        String fileioImpl = properties.get(CatalogProperties.FILE_IO_IMPL);
        Preconditions.checkArgument(fileioImpl.equals(LakeFSProperties.LAKEFS_FILE_IO_IMPL),
                "FileIO impl must be lakeFS FileIO");
        this.lakeFSRepo = StringUtils.substringAfter(LocationUtil.stripTrailingSlash(lakefsRepositoryURI), "//");
        String s3aURI = lakefsRepositoryURI.replaceFirst(LAKEFS_SCHEME, "s3a");
        if (properties.get(LakeFSProperties.URI_PREFIX_REPLACE_FROM) != null && !properties.get(LakeFSProperties.URI_PREFIX_REPLACE_FROM).isEmpty()) {
            s3aURI = lakefsRepositoryURI.replaceFirst(properties.get(LakeFSProperties.URI_PREFIX_REPLACE_FROM),
                    properties.get(LakeFSProperties.URI_PREFIX_REPLACE_TO));
        }
        Builder<String, String> newPropertiesBuilder = ImmutableMap.<String, String>builder();
        properties.forEach((key, value) -> {
            if (!key.equals(CatalogProperties.WAREHOUSE_LOCATION)) {
                newPropertiesBuilder.put(key, value);
            }
        });
        newPropertiesBuilder.put(CatalogProperties.WAREHOUSE_LOCATION, s3aURI);
        if (properties.containsKey(LakeFSProperties.WAREHOUSE_PREFIX)) {
            LOG.info("LakeFS catalog initialized with warehouse prefix: {}",
                    properties.get(LakeFSProperties.WAREHOUSE_PREFIX));
        }
        super.initialize(name, newPropertiesBuilder.build());
    }

    @Override
    protected TableOperations newTableOps(TableIdentifier identifier) {
//        String lakeFSBranch = StringUtils.substringBetween(identifier.toString(), "/", "."); // exctract branch from table
        String lakeFSBranch = "branch-a";
        return new LakeFSTableOperations(
                new Path(defaultWarehouseLocation(identifier)), fileIO, conf, lockManager, this.lakeFSRepo, lakeFSBranch);
    }

    @Override
    protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
        String tableName = tableIdentifier.name();
        StringBuilder sb = new StringBuilder();

        String[] levels = tableIdentifier.namespace().levels();
        for (int i = 1; i < levels.length; i++) {
            if (i > 0){
                sb.append(levels[i]).append('/');
            }
        }
        sb.append(tableName);

        return sb.toString();
    }
}
