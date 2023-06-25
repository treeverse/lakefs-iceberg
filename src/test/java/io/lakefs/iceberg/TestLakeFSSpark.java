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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class TestLakeFSSpark {
        public Dataset<Row> createRandomDataFrame(int numRows) {
            // Create a Spark session
            SparkSession spark = SparkSession.builder()
                    .appName("RandomDataFrameCreator")
                    .master("local[*]")
                    .getOrCreate();

            // Define the schema for the DataFrame
            StructType schema = DataTypes.createStructType(new StructField[]{
                    DataTypes.createStructField("col1", DataTypes.IntegerType, false),
                    DataTypes.createStructField("col2", DataTypes.IntegerType, false),
                    DataTypes.createStructField("col3", DataTypes.IntegerType, false),
                    DataTypes.createStructField("col4", DataTypes.StringType, false),
                    DataTypes.createStructField("col5", DataTypes.StringType, false)
            });

            // Generate random data and create rows for the DataFrame
            List<Row> rows = new ArrayList<>();
            Random random = new Random();
            for (int i = 0; i < numRows; i++) {
                int col1 = random.nextInt();
                int col2 = random.nextInt();
                int col3 = random.nextInt();
                String col4 = String.valueOf(random.nextInt());
                String col5 = String.valueOf(random.nextInt());
                Row row = RowFactory.create(col1, col2, col3, col4, col5);
                rows.add(row);
            }

            // Create the DataFrame
            Dataset<Row> df = spark.createDataFrame(rows, schema);

            // Return the DataFrame
            return df;
        }

    @Test
    public void testLakeFSWithSpark() throws NoSuchTableException {
        SparkConf conf = new SparkConf();
        conf.set("spark.sql.catalog.lakefs", "org.apache.iceberg.spark.SparkCatalog");
        conf.set("spark.sql.catalog.lakefs.catalog-impl", "io.lakefs.iceberg.LakeFSCatalog");
        conf.set("spark.sql.catalog.lakefs.warehouse", "lakefs://example-repo");
        conf.set("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");

        conf.set("spark.hadoop.fs.s3a.access.key", "KEY");
        conf.set("spark.hadoop.fs.s3a.secret.key", "SECRET");
        conf.set("spark.hadoop.fs.s3a.endpoint", "http://localhost:8000");
        conf.set("spark.hadoop.fs.s3a.path.style.access", "true");

        conf.set("spark.sql.files.maxPartitionBytes","402653184");
        SparkSession spark = SparkSession.builder().master("local").config(conf).getOrCreate();
    
        String catalog = "lakefs";
        String db = "db";
        String table = "mytable10";
        String branch = "main";
        spark.sql(String.format("CREATE SCHEMA IF NOT EXISTS %s.%s.%s", catalog, branch, db));
//        spark.sql(String.format("CREATE TABLE IF NOT EXISTS  %s.%s.%s.%s (col1 int,col2 int,col3 int,col4 string,col5 string) TBLPROPERTIES ('write.parquet.row-group-size-bytes'='402653184','read.split.target-size'='402653184') OPTIONS ('format-version'=2)", catalog, branch, db, table));
        spark.sql(String.format("CREATE TABLE IF NOT EXISTS  %s.%s.%s.%s (col1 int,col2 int,col3 int,col4 string,col5 string) OPTIONS ('format-version'=2)", catalog, branch, db, table));
        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("col1", DataTypes.IntegerType, false),
                DataTypes.createStructField("col2", DataTypes.IntegerType, false),
                DataTypes.createStructField("col3", DataTypes.IntegerType, false),
                DataTypes.createStructField("col4", DataTypes.StringType, false),
                DataTypes.createStructField("col5", DataTypes.StringType, false),
        });
        int size = 10000000;
        Dataset<Row> df = createRandomDataFrame(size);
        df.writeTo(String.format("%s.%s.%s.%s", catalog, branch, db, table)).append();

        spark.sql(String.format("SELECT * FROM %s.%s.%s.%s", catalog, branch, db, table)).show();
    }
}
