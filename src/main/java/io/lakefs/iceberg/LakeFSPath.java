package io.lakefs.iceberg;

import org.apache.hadoop.fs.Path;

public class LakeFSPath extends Path {
    public LakeFSPath(String pathString) throws IllegalArgumentException {
        super(pathString);
    }

    @Override
    public String toString() {
        return Util.GetPathFromURL(super.toString());
    }
}
