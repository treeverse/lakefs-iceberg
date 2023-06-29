package io.lakefs.iceberg;

import org.apache.hadoop.fs.Path;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

import java.util.Map;

public class LakeFSFileIO implements FileIO {

    private HadoopFileIO wrapped;
    private String lakeFSRepo;
    private String lakeFSRef;

    @SuppressWarnings("unused")
    public LakeFSFileIO() {
    }

    public LakeFSFileIO(HadoopFileIO wrapped, String lakeFSRepo, String lakeFSRef) {
        this.wrapped = wrapped;
        this.lakeFSRepo = lakeFSRepo;
        this.lakeFSRef = lakeFSRef;
    }

    @Override
    public Map<String, String> properties() {
        return wrapped.properties();
    }


    @Override
    public InputFile newInputFile(String path) {
        if (!path.startsWith("s3a://")) {
            path = String.format("s3a://%s/%s/%s", lakeFSRepo, lakeFSRef, path);
        }
        return HadoopInputFile.fromPath(new Path(path) {
            @Override
            public String toString() {
                return Util.GetPathFromURL(super.toString());
            }
        }, wrapped.conf());
    }

    @Override
    public InputFile newInputFile(String path, long length) {
        if (!path.startsWith("s3a://")) {
            path = String.format("s3a://%s/%s/%s", lakeFSRepo, lakeFSRef, path);
        }
        return HadoopInputFile.fromPath(new Path(path) {
            @Override
            public String toString() {
                return Util.GetPathFromURL(super.toString());
            }
        }, length, wrapped.conf());
    }

    @Override
    public OutputFile newOutputFile(String path) {
        if (!path.startsWith("s3a://")) {
            path = String.format("s3a://%s/%s/%s", lakeFSRepo, lakeFSRef, path);
        }
        return HadoopOutputFile.fromPath(new Path(path) {
            @Override
            public String toString() {
                return Util.GetPathFromURL(super.toString());
            }
        }, wrapped.conf());
    }

    @Override
    public void deleteFile(String path) {
        wrapped.deleteFile(path);
    }
}
