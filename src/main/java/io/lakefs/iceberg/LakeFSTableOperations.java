package io.lakefs.iceberg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.hadoop.HadoopTableOperations;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.util.LockManagers;

public class LakeFSTableOperations extends HadoopTableOperations {
    private LakeFSReporter reporter;
    FileIO fileIO;

    public LakeFSTableOperations(Path location, FileIO fileIO, LakeFSReporter reporter, Configuration conf) {
        super(location, fileIO, conf, LockManagers.defaultLockManager());
        this.fileIO = fileIO;
        this.reporter = reporter;
    }

    @Override
    public FileIO io() {
        return fileIO;
    }


    @Override
    public String metadataFileLocation(String fileName) {
        String path = super.metadataFileLocation(fileName);
        if (path.startsWith("s3a://")) {
            path = Util.GetPathFromURL(path);
        }
        return path;
    }

    @Override
    public void commit(TableMetadata base, TableMetadata metadata) {
        super.commit(base, metadata);
        reporter.logOp("commit");
    }

}
