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

def test_diff_two_same_branches(spark, lfs_client, lakefs_repo):
    print("repo name ", lakefs_repo)
    df = spark.read.option("inferSchema","true").option("multiline","true").csv("./test/data-sets/film_permits.csv")
    df.write.saveAsTable("lakefs.main.nyc.permits")

    #Commit, create a new branch, check that the tables are the same
    lfs_client.commits.commit(lakefs_repo, "main", CommitCreation(message="Initial data load"))
    lfs_client.branches.create_branch(lakefs_repo, BranchCreation(name="dev", source="main"))
    df_main = spark.read.table("lakefs.main.nyc.permits")
    df_dev = spark.read.table("lakefs.dev.nyc.permits")
    assert (df_main.schema == df_dev.schema) and (df_main.collect() == df_dev.collect()), "main and dev tables should be equal"
