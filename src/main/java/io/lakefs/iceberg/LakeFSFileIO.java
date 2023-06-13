package io.lakefs.iceberg;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

public class LakeFSFileIO extends HadoopFileIO {
    private String lakeFSRepo;
    private String lakeFSRef;

    public LakeFSFileIO(Configuration hadoopConf, String lakeFSRepo, String lakeFSRef) {
        super(hadoopConf);
        this.lakeFSRepo = lakeFSRepo;
        this.lakeFSRef = lakeFSRef;
    }

    @Override
    public InputFile newInputFile(String path) {
        if (!path.startsWith("s3a://")){
            path = "s3a://" + lakeFSRepo + "/" + lakeFSRef + "/" + path;
        }
        return new LakeFSInputFile(super.newInputFile(path));
    }

    @Override
    public InputFile newInputFile(String path, long length) {
        if (!path.startsWith("s3a://")){
            path = "s3a://" + lakeFSRepo + "/" + lakeFSRef + "/" + path;
        }
        return new LakeFSInputFile(super.newInputFile(path, length));
    }

    @Override
    public OutputFile newOutputFile(String path) {
        if (!path.startsWith("s3a://")){
            path = "s3a://" + lakeFSRepo + "/" + lakeFSRef + "/" + path;
        }
        return new LakeFSOutputFile(super.newOutputFile(path));
    }

    @Override
    public void deleteFile(String path){
        super.deleteFile(path);
    }
}
