package io.lakefs.iceberg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.hadoop.HadoopTableOperations;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.util.LockManagers;

public class LakeFSTableOperations extends HadoopTableOperations {
    FileIO fileIO;

    public LakeFSTableOperations(Path location, FileIO fileIO, Configuration conf) {
        super(location, fileIO, conf, LockManagers.defaultLockManager());
        this.fileIO = fileIO;
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
}
