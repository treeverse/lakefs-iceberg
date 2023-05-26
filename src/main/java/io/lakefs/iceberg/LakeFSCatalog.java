package io.lakefs.iceberg;

import java.util.Map;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class LakeFSCatalog extends HadoopCatalog {
    private static final Logger LOG = LoggerFactory.getLogger(LakeFSCatalog.class);

    @Override
    public void initialize(String name, Map<String, String> properties) {
        String lakefsRepositoryURI = properties.get(properties.get(CatalogProperties.WAREHOUSE_LOCATION));
        Preconditions.checkArgument(lakefsRepositoryURI.matches("lakefs://[^/]+"),
                "Warehouse path must be a lakeFS repository URI without a path (e.g. lakefs://example-repo)");
        String s3aURI = lakefsRepositoryURI.replaceFirst("lakefs://", "s3a://");
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, s3aURI);
        if (properties.containsKey(LakeFSProperties.WAREHOUSE_PREFIX)) {
            LOG.info("LakeFS catalog initialized with warehouse prefix: {}", properties.get(LakeFSProperties.WAREHOUSE_PREFIX));
        }
        super.initialize(name, properties);
    }
}
