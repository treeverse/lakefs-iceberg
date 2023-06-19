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

public class TestCatalogMigration {
    public static SparkConf newSparkSharedConfig(String repo)  {
        // per bucket config https://docs.cloudera.com/HDPDocuments/HDP3/HDP-3.1.4/bk_cloud-data-access/content/s3-per-bucket-configs.html
        SparkConf conf = new SparkConf();
        // set hive config
        conf.set("spark.sql.catalog.hadoop_prod", "org.apache.iceberg.spark.SparkCatalog");
        conf.set("spark.sql.catalog.hadoop_prod.type", "hadoop");
        conf.set("spark.sql.catalog.hadoop_prod.warehouse", "s3a://treeverse-ort-simulation-bucket/isan/iceberg-tests/");
        conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");
        conf.set("spark.hadoop.fs.s3a.bucket.treeverse-ort-simulation-bucket.access.key", "AWS_ACCESS_KEY_SOURCE_HIVE");
        conf.set("spark.hadoop.fs.s3a.bucket.treeverse-ort-simulation-bucket.secret.key", "AWS_SECRET_KEY_SOURCE_HIVE");
        conf.set("spark.hadoop.fs.s3a.path.style.access", "true");
        // set lakeFS config
        conf.set("spark.sql.catalog.lakefs", "org.apache.iceberg.spark.SparkCatalog");
        conf.set("spark.sql.catalog.lakefs.catalog-impl", "io.lakefs.iceberg.LakeFSCatalog");
        conf.set("spark.sql.catalog.lakefs.warehouse", String.format("lakefs://%s", repo));
        conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");
        conf.set(String.format("spark.hadoop.fs.s3a.bucket.%s.access.key", repo), "<LAKEFS_ACCESS_KEY>");
        conf.set(String.format("spark.hadoop.fs.s3a.bucket.%s.secret.key", repo), "<LAKEFS_SECRET_KEY>");
        conf.set(String.format("spark.hadoop.fs.s3a.bucket.%s.endpoint"  , repo), "<LAKEFS_ENDPOINT_URL>");
        return conf;
    }

    @Test
    public void testStandaloneIcebergSpark() throws NoSuchTableException {
        // directory-based catalog in HDFS that can be configured using type=hadoop
        SparkConf conf = new SparkConf();
        conf.set("spark.sql.catalog.hadoop_prod", "org.apache.iceberg.spark.SparkCatalog");
        conf.set("spark.sql.catalog.hadoop_prod.type", "hadoop");
        conf.set("spark.sql.catalog.hadoop_prod.warehouse", "s3a://treeverse-ort-simulation-bucket/isan/iceberg-tests/");
        conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");
        conf.set("spark.hadoop.fs.s3a.access.key", "AWS_ACCESS_KEY_SOURCE_HIVE");
        conf.set("spark.hadoop.fs.s3a.secret.key", "AWS_SECRET_KEY_SOURCE_HIVE");
        conf.set("spark.hadoop.fs.s3a.path.style.access", "true");

        SparkSession spark = SparkSession.builder().master("local").config(conf).getOrCreate();

        String catalog = "hadoop_prod";
        String db = "db";
        String table = "mytable";
        String branch = "main";
        spark.sql(String.format("CREATE SCHEMA IF NOT EXISTS %s.%s_%s", catalog, branch, db));
        spark.sql(String.format("CREATE TABLE IF NOT EXISTS  %s.%s_%s_%s (val int) OPTIONS ('format-version'=2)", catalog, branch, db, table));
        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("val", DataTypes.IntegerType, false)
        });
        Row row = RowFactory.create(10);
        Dataset<Row> df = spark.createDataFrame(List.of(row), schema).toDF("val");
        df.writeTo(String.format("%s.%s_%s_%s", catalog, branch, db, table)).append();
        spark.sql(String.format("SELECT * FROM %s.%s_%s_%s", catalog, branch, db, table)).show();
    }
    @Test
    public void testCopyTablesFromIcebergToLakeFS() throws NoSuchTableException {
        // connect to hadoop iceberg (none lakeFS)
        String catalog = "hadoop_prod";
        String lakeFSCatalog = "lakefs";
        String db = "db";
        String table = "mytable";
        String branch = "main";
//        String lakeFSRepo = "iceberg-one";
        String lakeFSRepo = "iceberg-two";
        SparkConf conf = TestCatalogMigration.newSparkSharedConfig(lakeFSRepo);
        SparkSession spark = SparkSession.builder().master("local").config(conf).getOrCreate();
        // hive table
        System.out.println("showing Hive source data");
        String hiveFullTableName = String.format("%s.%s_%s_%s", catalog, branch, db, table);
        //spark.sql(String.format("SELECT * FROM %s.%s_%s_%s", catalog, branch, db, table)).show();

        System.out.println("showing lakeFS destination data");

        // lakeFS table
        String lakeFSFullTableName=String.format("%s.%s.%s.%s", lakeFSCatalog, branch, db, table);

        // create schema
        spark.sql(String.format("CREATE SCHEMA IF NOT EXISTS %s.%s.%s", lakeFSCatalog, branch, db));

        // clone table
        spark.sql(String.format("CREATE TABLE IF NOT EXISTS %s USING iceberg AS SELECT * FROM %s", lakeFSFullTableName,hiveFullTableName));

        // show cloned table in lakeFS
        spark.sql(String.format("SELECT * FROM %s.%s.%s.%s", lakeFSCatalog, branch, db, table)).show();
    }

}
