package io.lakefs.iceberg;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.util.Collections;

public class TestLakeFSSpark {
    @Test
    public void testLakeFSWithSpark() throws NoSuchTableException {
        SparkConf conf = new SparkConf();
        conf.set("spark.sql.catalog.lakefs", "org.apache.iceberg.spark.SparkCatalog");
        conf.set("spark.sql.catalog.lakefs.catalog-impl", "io.lakefs.iceberg.LakeFSCatalog");
        conf.set("spark.sql.catalog.lakefs.warehouse", "lakefs://example-repo");
        conf.set("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");

        conf.set("spark.hadoop.fs.s3a.access.key", "AKIAIOSFODNN7EXAMPLE");
        conf.set("spark.hadoop.fs.s3a.secret.key", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        conf.set("spark.hadoop.fs.s3a.endpoint", "http://localhost:8084");
        conf.set("spark.hadoop.fs.s3a.path.style.access", "true");

        SparkSession spark = SparkSession.builder().master("local").config(conf).getOrCreate();
    
        String catalog = "lakefs";
        String db = "db";
        String table = "mytable";
        String branch = "main";
        spark.sql(String.format("CREATE SCHEMA IF NOT EXISTS %s.%s.%s", catalog, branch, db));
        spark.sql(String.format("CREATE TABLE IF NOT EXISTS  %s.%s.%s.%s (val int) OPTIONS ('format-version'=2)", catalog, branch, db, table));
        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("val", DataTypes.IntegerType, false)
        });
        Row row = RowFactory.create(10);
        Dataset<Row> df = spark.createDataFrame(Collections.singletonList(row), schema).toDF("val");
        df.writeTo(String.format("%s.%s.%s.%s", catalog, branch, db, table)).append();
        spark.sql(String.format("SELECT * FROM %s.%s.%s.%s", catalog, branch, db, table)).show();
    }
}
