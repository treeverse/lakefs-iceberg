package io.lakefs.iceberg;

import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;

public class LakeFSOutputFile implements OutputFile {
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

    @Override
    public String location() {
        String location = wrapped.location();
        return Util.GetPathFromURL(location);
    }

    public InputFile toInputFile() {
        return wrapped.toInputFile();
    }
}
