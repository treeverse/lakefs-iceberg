package io.lakefs.iceberg;

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
        return Util.GetPathFromURL(location);
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
