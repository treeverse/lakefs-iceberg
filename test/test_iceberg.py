import pytest

import lakefs_client
from lakefs_client.client import LakeFSClient
from lakefs_client.models import *
from lakefs_client.model.access_key_credentials import AccessKeyCredentials
from lakefs_client.model.comm_prefs_input import CommPrefsInput
from lakefs_client.model.setup import Setup
from lakefs_client.model.repository_creation import RepositoryCreation
import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def get_data(spark):
    data_set = [(1, "James", "Smith", 32, "M"),
                (2, "Michael","Rose", 35 ,"M"),
                (3, "Robert", "Williams", 41, "M"),
                (4, "Maria", "Jones", 36, "F"),
                (5, "Jen","Brown", 44, "F"),
                (6, "Monika","Geller", 31, "F")]

    schema = StructType([StructField("id", StringType(), True),
        StructField("firstname",StringType(),True),
        StructField("lastname",StringType(),True),
        StructField("age", IntegerType(), True),
        StructField("gender", StringType(), True),
        ])
    df = spark.createDataFrame(data=data_set,schema=schema)
    df.printSchema()
    df.show(truncate=False)
    return df

def test_diff_two_same_branches(spark, lfs_client, lakefs_repo):
    print("repo name ", lakefs_repo)
    df = get_data(spark)
    df.write.saveAsTable("lakefs.main.company.workers")

    #Commit, create a new branch, check that the tables are the same
    lfs_client.commits.commit(lakefs_repo, "main", CommitCreation(message="Initial data load"))
    lfs_client.branches.create_branch(lakefs_repo, BranchCreation(name="dev", source="main"))
    df_main = spark.read.table("lakefs.main.company.workers")
    df_dev = spark.read.table("lakefs.dev.company.workers")
    assert (df_main.schema == df_dev.schema) and (df_main.collect() == df_dev.collect()), "main and dev tables should be equal"

def test_delete_on_dev_and_merge(spark, lfs_client, lakefs_repo):
    lfs_client.branches.create_branch(lakefs_repo, BranchCreation(name="test1", source="main"))
    spark.sql("DELETE FROM lakefs.test1.company.workers WHERE id = 6")
    lfs_client.commits.commit(lakefs_repo, "test1", CommitCreation(message="delete one row"))
    lfs_client.refs.merge_into_branch(lakefs_repo, "test1", "main")
    df_main = spark.read.table("lakefs.main.company.workers")
    df_dev = spark.read.table("lakefs.test1.company.workers")
    assert (df_main.schema == df_dev.schema) and (df_main.collect() == df_dev.collect()), "main and test1 tables should be equal"


def test_multiple_changes_and_merge(spark, lfs_client, lakefs_repo):
    df = get_data(spark)
    df.write.saveAsTable("lakefs.main.company.workers")

    lfs_client.commits.commit(lakefs_repo, "main", CommitCreation(message="Initial data load"))
    lfs_client.branches.create_branch(lakefs_repo, BranchCreation(name="dev", source="main"))
    spark.sql("DELETE FROM lakefs.dev.company.workers WHERE id = 6")
    spark.sql("DELETE FROM lakefs.dev.company.workers WHERE id = 5")
    spark.sql("INSERT INTO lakefs.dev.company.workers VALUES (7, 'Jhon', 'Smith', 33, 'M')")
    spark.sql("DELETE FROM lakefs.dev.company.workers WHERE id = 4")
    spark.sql("INSERT INTO lakefs.dev.company.workers VALUES (8, 'Marta', 'Green', 31, 'F')")
    lfs_client.commits.commit(lakefs_repo, "dev", CommitCreation(message="Some changes"))
    lfs_client.refs.merge_into_branch(lakefs_repo, "dev", "main")
    df_main = spark.read.table("lakefs.main.company.workers")
    df_dev = spark.read.table("lakefs.dev.company.workers")
    assert (df_main.schema == df_dev.schema) and (df_main.collect() == df_dev.collect()), "main and dev tables should be equal"