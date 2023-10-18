package io.lakefs.iceberg;

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
        if (!path.startsWith(String.format("s3a://%s/%s/", lakeFSRepo, lakeFSRef))) {
            System.out.println(String.format("path %s does not start with s3a://%s/%s/", path, lakeFSRepo, lakeFSRef));
            // not a path in the repository, treat as a regular path
            return wrapped.newInputFile(path);
        }
        System.out.println(String.format("path %s starts with s3a://%s/%s/", path, lakeFSRepo, lakeFSRef));
        return HadoopInputFile.fromPath(new LakeFSPath(path), wrapped.conf());
    }

    @Override
    public InputFile newInputFile(String path, long length) {
        if (!path.startsWith("s3a://")) {
            path = String.format("s3a://%s/%s/%s", lakeFSRepo, lakeFSRef, path);
        }
        if (!path.startsWith(String.format("s3a://%s/%s/", lakeFSRepo, lakeFSRef))) {
            // not a path in the repository, treat as a regular path
            System.out.println(String.format("path %s does not start with s3a://%s/%s/", path, lakeFSRepo, lakeFSRef));
            return wrapped.newInputFile(path, length);
        }
        System.out.println(String.format("path %s starts with s3a://%s/%s/", path, lakeFSRepo, lakeFSRef));
        return HadoopInputFile.fromPath(new LakeFSPath(path), length, wrapped.conf());
    }

    @Override
    public OutputFile newOutputFile(String path) {
        if (!path.startsWith("s3a://")) {
            path = String.format("s3a://%s/%s/%s", lakeFSRepo, lakeFSRef, path);
        }
        if (!path.startsWith(String.format("s3a://%s/%s/", lakeFSRepo, lakeFSRef))) {
            System.out.println(String.format("path %s does not start with s3a://%s/%s/", path, lakeFSRepo, lakeFSRef));
            // not a path in the repository, treat as a regular path
            return wrapped.newOutputFile(path);
        }
        System.out.println(String.format("path %s starts with s3a://%s/%s/", path, lakeFSRepo, lakeFSRef));
        return HadoopOutputFile.fromPath(new LakeFSPath(path), wrapped.conf());
    }

    @Override
    public void deleteFile(String path) {
        wrapped.deleteFile(path);
    }
}
