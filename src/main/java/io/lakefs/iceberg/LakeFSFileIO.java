package io.lakefs.iceberg;

import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.util.SerializableMap;
import org.apache.iceberg.util.SerializableSupplier;

import java.util.Map;

public class LakeFSFileIO implements FileIO {

    private FileIO wrapped;
    private String lakeFSRepo;
    private String lakeFSRef;
    private SerializableSupplier<Configuration> hadoopConf;
    private SerializableMap<String, String> properties = SerializableMap.copyOf(ImmutableMap.of());

    public LakeFSFileIO() {
    }

    public LakeFSFileIO(FileIO wrapped, String lakeFSRepo, String lakeFSRef) {
        this.wrapped = wrapped;
        this.lakeFSRepo = lakeFSRepo;
        this.lakeFSRef = lakeFSRef;
    }

    public LakeFSFileIO(FileIO wrapped, String lakeFSRepo, String lakeFSRef, SerializableSupplier<Configuration> hadoopConf) {
        this.wrapped = wrapped;
        this.lakeFSRepo = lakeFSRepo;
        this.lakeFSRef = lakeFSRef;
        this.hadoopConf = hadoopConf;
    }

    @Override
    public void initialize(Map<String, String> props) {
        this.properties = SerializableMap.copyOf(props);
    }

    @Override
    public Map<String, String> properties() {
        return wrapped.properties();
    }


    @Override
    public InputFile newInputFile(String path) {
        if (!path.startsWith("s3a://")){
            path = String.format("s3a://%s/%s/%s", lakeFSRepo, lakeFSRef, path);
        }
        return new LakeFSInputFile(wrapped.newInputFile(path));
    }

    @Override
    public InputFile newInputFile(String path, long length) {
        if (!path.startsWith("s3a://")){
            path = String.format("s3a://%s/%s/%s", lakeFSRepo, lakeFSRef, path);
        }
        return new LakeFSInputFile(wrapped.newInputFile(path, length));
    }

    @Override
    public OutputFile newOutputFile(String path) {
        if (!path.startsWith("s3a://")){
            path = String.format("s3a://%s/%s/%s", lakeFSRepo, lakeFSRef, path);
        }
        return new LakeFSOutputFile(wrapped.newOutputFile(path));
    }

    @Override
    public void deleteFile(String path){
        wrapped.deleteFile(path);
    }
}
