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

import java.util.List;

public class TestLakeFSSpark {
    @Test
    public void testLakeFSWithSpark() throws NoSuchTableException {
        SparkConf conf = new SparkConf();
        conf.set("spark.sql.catalog.hadoop", "org.apache.iceberg.spark.SparkCatalog");
        conf.set("spark.sql.catalog.hadoop.type", "hadoop");
        conf.set("spark.sql.catalog.hadoop.io-impl", "io.lakefs.iceberg.LakeFSFileIO");
        conf.set("spark.sql.catalog.hadoop.warehouse", "s3a://example-repo/main/wh/");
        conf.set("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");

        conf.set("spark.hadoop.fs.s3a.access.key", "");
        conf.set("spark.hadoop.fs.s3a.secret.key", "");

        conf.set("spark.hadoop.fs.s3a.endpoint", "http://localhost:8000");
        conf.set("spark.hadoop.fs.s3a.path.style.access", "true");

        SparkSession spark = SparkSession.builder().master("local").config(conf).getOrCreate();
    
        String catalog = "hadoop";
        String table = "example-table";
        spark.sql(String.format("CREATE SCHEMA IF NOT EXISTS %s.db", catalog));
        spark.sql(String.format("CREATE TABLE  IF NOT EXISTS  %s.db.`%s` (val int) OPTIONS ('format-version'=2)", catalog, table));
        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("val", DataTypes.IntegerType, false)
        });
        Row row = RowFactory.create(10);
        Dataset<Row> df = spark.createDataFrame(List.of(row), schema).toDF("val");
        df.writeTo(String.format("%s.db.`%s`", catalog, table)).append();
        spark.sql(String.format("SELECT * FROM %s.db.`%s`", catalog, table)).show();
    
    }
}