package io.lakefs.iceberg;

import org.apache.hadoop.fs.Path;

/**
 * This is a Hadoop path with a dedicated {@link #toString()} implementation for lakeFS.
 * The path is assumed to be a lakeFS path of the form <code>[scheme]://[repo-name]/[ref-name]/rest/of/path/</code>.
 */
public class LakeFSPath extends Path {
    public LakeFSPath(String pathString) throws IllegalArgumentException {
        super(pathString);
    }

    /**
     * Return the path relative to the lakeFS repository root.
     * For example, given <code>s3a://example-repo/main/a/b/c</code>, return <code>a/b/c</code>.
     */
    @Override
    public String toString() {
        return Util.GetPathFromURL(super.toString());
    }
}
