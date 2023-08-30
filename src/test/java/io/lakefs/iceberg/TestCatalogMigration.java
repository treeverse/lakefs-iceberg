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
import java.util.HashMap;

import static org.junit.Assert.assertTrue;

public class TestCatalogMigration {
    public static SparkConf newSparkSharedConfig(HashMap<String,String> lakeFSConf,  HashMap<String, String> srcConf)  {
        // per bucket config https://docs.cloudera.com/HDPDocuments/HDP3/HDP-3.1.4/bk_cloud-data-access/content/s3-per-bucket-configs.html
        SparkConf conf = new SparkConf();
        conf.set("spark.hadoop.fs.s3a.path.style.access", "true");
        if (srcConf != null){
            // set hive config
            conf.set("spark.sql.catalog.hadoop_prod", "org.apache.iceberg.spark.SparkCatalog");
            conf.set("spark.sql.catalog.hadoop_prod.type", "hadoop");
            conf.set("spark.sql.catalog.hadoop_prod.warehouse", String.format("s3a://%s/%s", srcConf.get("bucket"), srcConf.get("base_path")));
            conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");
            conf.set(String.format("spark.hadoop.fs.s3a.bucket.%s.access.key", srcConf.get("bucket")), srcConf.get("aws_access_key"));
            conf.set(String.format("spark.hadoop.fs.s3a.bucket.%s.secret.key", srcConf.get("bucket")), srcConf.get("aws_secret_key"));
        }
        if (lakeFSConf != null) {
            // set lakeFS config
            conf.set("spark.sql.catalog.lakefs", "org.apache.iceberg.spark.SparkCatalog");
            conf.set("spark.sql.catalog.lakefs.catalog-impl", "io.lakefs.iceberg.LakeFSCatalog");
            conf.set("spark.sql.catalog.lakefs.warehouse", String.format("lakefs://%s", lakeFSConf.get("repo")));
            conf.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");
            conf.set(String.format("spark.hadoop.fs.s3a.bucket.%s.access.key", lakeFSConf.get("repo")), lakeFSConf.get("access_key"));
            conf.set(String.format("spark.hadoop.fs.s3a.bucket.%s.secret.key", lakeFSConf.get("repo")), lakeFSConf.get("secret_key"));
            conf.set(String.format("spark.hadoop.fs.s3a.bucket.%s.endpoint"  , lakeFSConf.get("repo")), lakeFSConf.get("endpoint"));
        }
        return conf;
    }

    @Test
    public void testMigrateHadoopToLakeFSCatalog() throws NoSuchTableException {

        String catalog = "hadoop_prod";
        String db = "db";
        String table = "mytable";
        String branch = "main";
        String lakeFSCatalog = "lakefs";
        String lakeFSRepo = "<LAKEFS_TABLE_NAME>";

        // hadoop catalog on s3 iceberg config (source)
        HashMap<String, String> hadoopConf = new HashMap<>();
        hadoopConf.put("bucket", "treeverse-ort-simulation-bucket");
        hadoopConf.put("base_path", "isan/iceberg-tests2/");
        hadoopConf.put("aws_access_key","<AWS_ACCESS_KEY>");
        hadoopConf.put("aws_secret_key","<AWS_SECRET_KEY>");

        // lakefs catalog iceberg config (destination)
        HashMap<String, String> lakeFSConf = new HashMap<>();
        lakeFSConf.put("access_key", "<LAKEFS_ACCESS_KEY>");
        lakeFSConf.put("secret_key", "<LAKEFS_SECRET_KEY>");
        lakeFSConf.put("endpoint", "<LAKEFS_ENDPOINT>");
        lakeFSConf.put("repo", lakeFSRepo);

        // pre-lakeFS simulate hadoop catalog on s3 catalog iceberg table

        // create spark session for hadoop on s3 catalog
        SparkConf conf = TestCatalogMigration.newSparkSharedConfig(null, hadoopConf);
        SparkSession spark = SparkSession.builder().master("local").config(conf).getOrCreate();

        // create table in hadoop catalog
        spark.sql(String.format("CREATE SCHEMA IF NOT EXISTS %s.%s", catalog,db));
        spark.sql(String.format("CREATE TABLE IF NOT EXISTS  %s.%s_%s (val int) OPTIONS ('format-version'=2)", catalog, db, table));
        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("val", DataTypes.IntegerType, false)
        });

        // populate with data
        Row row = RowFactory.create(10);
        Dataset<Row> df = spark.createDataFrame(Collections.singletonList(row), schema).toDF("val");
        df.writeTo(String.format("%s.%s_%s", catalog, db, table)).append();

        // show created data
        spark.sql(String.format("SELECT * FROM %s.%s_%s", catalog, db, table));

        // stop the hadoop spark session
        spark.stop();

        // simulate migration into lakeFS catalog for an iceberg table

        String hiveFullTableName = String.format("%s.%s_%s", catalog, db, table);
        String lakeFSFullTableName = String.format("%s.%s.%s.%s", lakeFSCatalog, branch, db, table);

        // create spark session that is configured to both source (hadoop catalog) and lakeFS catalog.

        SparkConf sharedConf = TestCatalogMigration.newSparkSharedConfig(lakeFSConf, hadoopConf);
        SparkSession sharedSpark = SparkSession.builder().master("local").config(sharedConf).getOrCreate();

        // create schema in lakeFS
        sharedSpark.sql(String.format("CREATE SCHEMA IF NOT EXISTS %s.%s.%s", lakeFSCatalog, branch, db));

        // migrate iceberg table into lakeFS
        sharedSpark.sql(String.format("CREATE TABLE IF NOT EXISTS %s USING iceberg AS SELECT * FROM %s", lakeFSFullTableName,hiveFullTableName));

        // show cloned table in lakeFS
        Dataset<Row> lakeFSDf;
        lakeFSDf = sharedSpark.sql(String.format("SELECT * FROM %s.%s.%s.%s", lakeFSCatalog, branch, db, table));
        lakeFSDf.show();

        // assert source and target tables are equal
        Dataset<Row> diff = lakeFSDf.except(sharedSpark.sql(String.format("SELECT * FROM %s", hiveFullTableName)));
        assertTrue(diff.isEmpty());
    }

}
