package io.lakefs.iceberg;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.util.LocationUtil;
import java.util.Map;

public class LakeFSCatalog extends HadoopCatalog {
    private static final String LAKEFS_SCHEME = "lakefs";
    private String lakeFSRepo;

    @Override
    public void initialize(String name, Map<String, String> properties) {
        String lakefsRepositoryURI = properties.get(CatalogProperties.WAREHOUSE_LOCATION);
        Preconditions.checkArgument(lakefsRepositoryURI.matches("lakefs://[^/]+"),
                "Warehouse path must be a lakeFS repository URI without a path (e.g. lakefs://example-repo)");
        lakeFSRepo = StringUtils.substringAfter(LocationUtil.stripTrailingSlash(lakefsRepositoryURI), "//");
        String s3aURI = lakefsRepositoryURI.replaceFirst(LAKEFS_SCHEME, "s3a");
        if (properties.get(LakeFSProperties.URI_PREFIX_REPLACE_FROM) != null && !properties.get(LakeFSProperties.URI_PREFIX_REPLACE_FROM).isEmpty()) {
            s3aURI = lakefsRepositoryURI.replaceFirst(properties.get(LakeFSProperties.URI_PREFIX_REPLACE_FROM),
                    properties.get(LakeFSProperties.URI_PREFIX_REPLACE_TO));
        }
        Builder<String, String> newPropertiesBuilder = ImmutableMap.builder();
        properties.forEach((key, value) -> {
            if (!key.equals(CatalogProperties.WAREHOUSE_LOCATION)) {
                newPropertiesBuilder.put(key, value);
            }
        });
        newPropertiesBuilder.put(CatalogProperties.WAREHOUSE_LOCATION, s3aURI);
        super.initialize(name, newPropertiesBuilder.build());
    }

    public Map<String, String> loadNamespaceMetadata(Namespace namespace) {
        return ImmutableMap.of("location",
                new Path("s3a://" + lakeFSRepo + "/", StringUtils.join(namespace.levels(), "/")).toString());
    }

    @Override
    protected TableOperations newTableOps(TableIdentifier identifier) {
        Preconditions.checkArgument(
                identifier.namespace().levels().length >= 2, String.format("Missing database in table identifier: %s", identifier));
        String lakeFSRef = identifier.namespace().levels()[identifier.namespace().length() - 2]; // TODO(yoni) just an example - test this
        Configuration conf = getConf();
        LakeFSFileIO fileIO = new LakeFSFileIO(new HadoopFileIO(conf), lakeFSRepo, lakeFSRef);
        String location = String.format("s3a://%s/%s/%s", lakeFSRepo, lakeFSRef, defaultWarehouseLocation(identifier));
        LakeFSReporter reporter = new LakeFSReporter(conf);
        return new LakeFSTableOperations(new Path(location), fileIO, reporter, conf);
    }

    @Override
    protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
        String tableName = tableIdentifier.name();
        StringBuilder sb = new StringBuilder();
        String[] levels = tableIdentifier.namespace().levels();
        for (int i = 1; i < levels.length; i++) {
            sb.append(levels[i]).append('/');
        }
        sb.append(tableName);
        return sb.toString();
    }
}
