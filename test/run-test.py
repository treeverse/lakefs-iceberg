import argparse
import sys

import lakefs_client
from lakefs_client import models
from lakefs_client.client import LakeFSClient
# from python_on_whales import docker
import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

from tenacity import retry, stop_after_attempt, wait_fixed

@retry(wait=wait_fixed(1), stop=stop_after_attempt(7))
def wait_for_setup(lfs_client):
    setup_state = lfs_client.config.get_setup_state()
    assert setup_state.state == 'initialized'

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--storage_namespace", default="local://", required=True)
    parser.add_argument("--repository", default="example")
    # parser.add_argument("--aws_access_key")
    # parser.add_argument("--aws_secret_key")
    # parser.add_argument("--access_mode", choices=["s3_gateway", "hadoopfs", "hadoopfs_presigned"], default="s3_gateway")
    lakefs_access_key = 'AKIAIOSFODNN7EXAMPLE'
    lakefs_secret_key = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'

    args = parser.parse_args()

    lfs_client = LakeFSClient(
        lakefs_client.Configuration(username=lakefs_access_key,
                                    password=lakefs_secret_key,
                                    host='http://localhost:8000'))
    wait_for_setup(lfs_client)
    lfs_client.repositories.create_repository(
        models.RepositoryCreation(name=args.repository,
                                  storage_namespace=args.storage_namespace,
                                  default_branch='main',))

    spark_config = SparkConf()
    spark_config.set("spark.sql.catalog.lakefs", "org.apache.iceberg.spark.SparkCatalog")
    spark_config.set("spark.sql.catalog.lakefs.catalog-impl", "io.lakefs.iceberg.LakeFSCatalog")
    spark_config.set("spark.sql.catalog.lakefs.warehouse", f"lakefs://${args.repository}")
    spark_config.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    spark_config.set("spark.hadoop.fs.s3a.endpoint", "http://localhost:8000")
    spark_config.set("spark.hadoop.fs.s3a.access.key", lakefs_access_key)
    spark_config.set("spark.hadoop.fs.s3a.secret.key", lakefs_secret_key)
    spark_config.set("spark.hadoop.fs.s3a.path.style.access", "true")
    spark_config.set("spark.jars", "target/lakefs-iceberg-1.0-SNAPSHOT.jar")

    spark = SparkSession.builder.config(conf=spark_config).getOrCreate()

    df = spark.read.option("inferSchema","true").option("multiline","true").csv("./data-sets/film_permits.csv")
    df.write.saveAsTable("lakefs.main.nyc.permits")



if __name__ == '__main__':
    main()