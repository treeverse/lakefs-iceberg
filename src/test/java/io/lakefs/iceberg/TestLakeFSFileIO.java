package io.lakefs.iceberg;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.InputFile;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestLakeFSFileIO {

    private LakeFSFileIO lakeFSFileIO;

    @Before
    public void setUp() {
    HadoopFileIO hadoopFileIO = new HadoopFileIO(new Configuration());
        String lakeFSRepo = "myLakeFSRepo";
        String lakeFSRef = "myLakeFSRef";
        lakeFSFileIO = new LakeFSFileIO(hadoopFileIO, lakeFSRepo, lakeFSRef);
    }

    @Test
    public void testNewInputFile() {
        // Test the behavior of newInputFile method
        String relativePath = "path/in/repo";
        String absolutePath = "s3a://myLakeFSRepo/myLakeFSRef/other/path/in/repo";
        String externalPath = "s3a://otherBucket/otherPath";
        InputFile relativeInputFile = lakeFSFileIO.newInputFile(relativePath);
        Assert.assertEquals("path/in/repo", relativeInputFile.location());
        InputFile absoluteInputFile = lakeFSFileIO.newInputFile(absolutePath);
        Assert.assertEquals("other/path/in/repo", absoluteInputFile.location());
        InputFile externalInputFile = lakeFSFileIO.newInputFile(externalPath);
        Assert.assertEquals("s3a://otherBucket/otherPath", externalInputFile.location());
    }
}
