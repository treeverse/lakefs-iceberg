package io.lakefs.iceberg;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

public class TestLakeFSSpark {
    @Test
    public void testLakeFSWithSpark() {
        SparkConf conf = new SparkConf();

        SparkSession spark = SparkSession.builder().master("local").config(conf).getOrCreate();
    
        String catalog = "lakefs";
        String table = "example-table";
        spark.sql(String.format("CREATE SCHEMA IF NOT EXISTS %s.db", catalog));
        spark.sql(String.format("CREATE TABLE  IF NOT EXISTS  %s.db.`%s` (val int) OPTIONS ('format-version'=2)", catalog, table));
        Dataset<Row> df = spark.createDataFrame(Arrays.asList("111", "222"), String.class).toDF("val");
        df.writeTo(String.format("%s.db.`%s`"));
        spark.sql(String.format("SELECT * FROM %s.db.`%s`", catalog, table)).show();
    
    }
}