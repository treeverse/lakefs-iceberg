package io.lakefs.iceberg;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.TestHadoopCatalog;

import com.google.common.collect.ImmutableMap;
// TODO this is a prep for testing the catalog

public class TestLakeFSCatalog extends TestHadoopCatalog {
    @Override
    protected HadoopCatalog hadoopCatalog(Map<String, String> catalogProperties) throws IOException {
        // temp.newFolder()
        LakeFSCatalog lakeFSCatalog = new LakeFSCatalog();
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        lakeFSCatalog.setConf(conf);
        File catalogDirectory = temp.newFolder();
        lakeFSCatalog.initialize(
                "hadoop",
                ImmutableMap.<String, String>builder()
                        .putAll(catalogProperties)
                        .put(CatalogProperties.WAREHOUSE_LOCATION, "lakefs://example-repo")
                        .put(LakeFSProperties.URI_PREFIX_REPLACE_FROM, "lakefs://")
                        .put(LakeFSProperties.URI_PREFIX_REPLACE_TO, catalogDirectory.getAbsolutePath())
                        .buildOrThrow());
        return lakeFSCatalog;
    }

}
