package io.lakefs.iceberg;


// TODO lynn: Go over import list

import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;

public class LakeFSTableOperations implements TableOperations {
    TableOperations wrapped;
    FileIO fileIO;

    public LakeFSTableOperations(TableOperations wrapped, String lakeFSRepo, String lakeFSRef) {
        this.wrapped = wrapped;
        this.fileIO = new LakeFSFileIO(wrapped.io(), lakeFSRepo, lakeFSRef);
    }

    @Override
    public TableMetadata current() {
        return wrapped.current();
    }

    @Override
    public TableMetadata refresh() {
        return wrapped.refresh();
    }

    @Override
    public void commit(TableMetadata base, TableMetadata metadata) {
        wrapped.commit(base, metadata);
    }

    @Override
    public FileIO io() {
        return fileIO;
    }

    @Override
    public String metadataFileLocation(String fileName) {
        String path = wrapped.metadataFileLocation(fileName);
        path = StringUtils.substringAfter(path, "//");
        path = StringUtils.substringAfter(path, "/");
        path = StringUtils.substringAfter(path, "/");
        return path;
    }

    @Override
    public LocationProvider locationProvider() {
        return wrapped.locationProvider();
    }
}
