import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import pytest

import lakefs_sdk
from lakefs_sdk.client import LakeFSClient
from lakefs_sdk.models import *

LAKEFS_ACCESS_KEY = 'AKIAIOSFODNN7EXAMPLE'
LAKEFS_SECRET_KEY = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
MOCK_EMAIL = "test@acme.co"

def pytest_addoption(parser):
    parser.addoption(
        '--storage_namespace', action='store', default='local://'
    )
    parser.addoption(
        '--repository', action='store', default='example'
    )


@pytest.fixture
def lakefs_repo(request):
    return request.config.getoption('--repository')


@pytest.fixture(scope="session")
def spark(pytestconfig):
    repo_name = pytestconfig.getoption('--repository')
    spark_config = SparkConf()
    spark_config.set("spark.sql.catalog.lakefs", "org.apache.iceberg.spark.SparkCatalog")
    spark_config.set("spark.sql.catalog.lakefs.catalog-impl", "io.lakefs.iceberg.LakeFSCatalog")
    spark_config.set("spark.sql.catalog.lakefs.warehouse", f"lakefs://{repo_name}")
    spark_config.set("spark.sql.catalog.lakefs.cache-enabled", "false")
    spark_config.set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    spark_config.set("spark.hadoop.fs.s3a.endpoint", "http://localhost:8000")
    spark_config.set("spark.hadoop.fs.s3a.access.key", LAKEFS_ACCESS_KEY)
    spark_config.set("spark.hadoop.fs.s3a.secret.key", LAKEFS_SECRET_KEY)
    spark_config.set("spark.hadoop.fs.s3a.path.style.access", "true")
    spark_config.set("spark.jars.packages", "io.lakefs:lakefs-iceberg:1.0-SNAPSHOT,org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.3.0,org.apache.hadoop:hadoop-aws:3.3.4,org.apache.hadoop:hadoop-client-api:3.3.4")

    spark = SparkSession.builder.config(conf=spark_config).getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture(scope="session")
def lfs_client(pytestconfig):
    lfs_client = LakeFSClient(
        lakefs_sdk.Configuration(username=LAKEFS_ACCESS_KEY,
                                    password=LAKEFS_SECRET_KEY,
                                    host='http://localhost:8000'))

    # Setup lakeFS
    repo_name = pytestconfig.getoption('--repository')
    storage_namespace = pytestconfig.getoption('--storage_namespace')
    lfs_client.internal_api.setup_comm_prefs(CommPrefsInput(feature_updates=False, security_updates=False, email=MOCK_EMAIL))
    lfs_client.internal_api.setup(Setup(username="admin",
                                  key=AccessKeyCredentials(access_key_id=LAKEFS_ACCESS_KEY, secret_access_key=LAKEFS_SECRET_KEY)))
    lfs_client.repositories_api.create_repository(
        RepositoryCreation(name=repo_name, storage_namespace=storage_namespace))
    return lfs_client
