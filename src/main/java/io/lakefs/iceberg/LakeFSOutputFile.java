package io.lakefs.iceberg;

import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;

public class LakeFSOutputFile implements OutputFile {

//    private final FileSystem fs;
//    private final Path path;
//    private final Configuration conf;
//    private NativeFileCryptoParameters nativeEncryptionParameters;

    private final OutputFile wrapped;
    
    public LakeFSOutputFile(OutputFile wrapped) {

        this.wrapped = wrapped;
    }


    @Override
    public PositionOutputStream create() {
        return wrapped.create();
    }

    @Override
    public PositionOutputStream createOrOverwrite() {
        return wrapped.createOrOverwrite();
    }

//    public Path getPath() {
//        return wrapped.getPath();
//    }
//
//    public Configuration getConf(){
//        return wrapped.getConf();
//    }

//    public FileSystem getFileSystem() {
//        return wrapped.getFileSystem();
//    }

    @Override
    public String location() {
        String location = wrapped.location();
        location = StringUtils.substringAfter(location, "//");
        location = StringUtils.substringAfter(location, "/");
        location = StringUtils.substringAfter(location, "/");
        return location;
    }

    public InputFile toInputFile() {
        return wrapped.toInputFile();
    }

//    public String toString() {
//        return location();
//    }
//
//    public NativeFileCryptoParameters nativeCryptoParameters() {
//        return wrapped.nativeCryptoParameters();
//    }
//
//
//    public void setNativeCryptoParameters(NativeFileCryptoParameters nativeCryptoParameters) {
//        wrapped.setNativeCryptoParameters(nativeCryptoParameters);
//    }
//
//
//    public Configuration conf() {
//
//        return wrapped.conf();
//    }
}
