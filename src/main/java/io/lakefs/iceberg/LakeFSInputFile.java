package io.lakefs.iceberg;

import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;

public class LakeFSInputFile implements InputFile {

    private final InputFile wrapped;
    
    public LakeFSInputFile(InputFile wrapped) {
        this.wrapped = wrapped;
    }
    
    @Override
    public String location() {
        String location = wrapped.location();
        location = StringUtils.substringAfter(location, "//");
        location = StringUtils.substringAfter(location, "/");
        location = StringUtils.substringAfter(location, "/");
        return location;
    }

    @Override
    public long getLength(){
        return wrapped.getLength();
    }

    @Override
    public SeekableInputStream newStream(){
        return wrapped.newStream();
    }

    @Override
    public boolean exists(){
        return wrapped.exists();
    }
}
