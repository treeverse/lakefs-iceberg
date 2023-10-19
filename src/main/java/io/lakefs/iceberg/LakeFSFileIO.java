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
        if (!path.matches("^[0-9a-z]*://.*")) {
            path = String.format("s3a://%s/%s/%s", lakeFSRepo, lakeFSRef, path);
        }
        if (!path.startsWith(String.format("s3a://%s/%s/", lakeFSRepo, lakeFSRef))) {
            // not a path in the repository, treat as a regular path
            return wrapped.newInputFile(path);
        }
        return HadoopInputFile.fromPath(new LakeFSPath(path), wrapped.conf());
    }

    @Override
    public InputFile newInputFile(String path, long length) {
        if (!path.matches("^[0-9a-z]*://.*")) {
            path = String.format("s3a://%s/%s/%s", lakeFSRepo, lakeFSRef, path);
        }
        if (!path.startsWith(String.format("s3a://%s/%s/", lakeFSRepo, lakeFSRef))) {
            // not a path in the repository, treat as a regular path
            return wrapped.newInputFile(path, length);
        }
        return HadoopInputFile.fromPath(new LakeFSPath(path), length, wrapped.conf());
    }

    @Override
    public OutputFile newOutputFile(String path) {
        if (!path.matches("^[0-9a-z]*://.*")) {
            path = String.format("s3a://%s/%s/%s", lakeFSRepo, lakeFSRef, path);
        }
        if (!path.startsWith(String.format("s3a://%s/%s/", lakeFSRepo, lakeFSRef))) {
            // not a path in the repository, treat as a regular path
            return wrapped.newOutputFile(path);
        }
        return HadoopOutputFile.fromPath(new LakeFSPath(path), wrapped.conf());
    }

    @Override
    public void deleteFile(String path) {
        wrapped.deleteFile(path);
    }
}
