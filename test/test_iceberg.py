import lakefs_sdk.client
from lakefs_sdk.models import *
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
    return df


def initiate_repo_with_data(spark, lfs_client: lakefs_sdk.client.LakeFSClient, repo_name, storage_namespace, test_name):
    storage_namespace = f"{storage_namespace}/{test_name}"
    print("storage_namespace ", storage_namespace)
    print("repo name ", repo_name)
    lfs_client.repositories_api.create_repository(
        RepositoryCreation(name=repo_name, storage_namespace=storage_namespace))
    df = get_data(spark)
    df.write.saveAsTable("lakefs.main.company.workers")
    lfs_client.commits_api.commit(repo_name, "main", CommitCreation(message="Initial data load"))


def test_diff_two_same_branches(spark, lfs_client: lakefs_sdk.client.LakeFSClient, lakefs_repo, storage_namespace):
    repo_name = f"{lakefs_repo}_test1"
    initiate_repo_with_data(spark, lfs_client, repo_name, storage_namespace, "test1")

    #Create a new branch, check that the tables are the same
    lfs_client.branches_api.create_branch(repo_name, BranchCreation(name="dev", source="main"))
    df_main = spark.read.table("lakefs.main.company.workers")
    df_dev = spark.read.table("lakefs.dev.company.workers")
    assert (df_main.schema == df_dev.schema) and (df_main.collect() == df_dev.collect()), "main and dev tables should be equal"


def test_delete_on_dev_and_merge(spark, lfs_client: lakefs_sdk.client.LakeFSClient, lakefs_repo, storage_namespace):
    repo_name = f"{lakefs_repo}_test2"
    initiate_repo_with_data(spark, lfs_client, repo_name, storage_namespace, "test2")

    lfs_client.branches_api.create_branch(repo_name, BranchCreation(name="test2", source="main"))
    spark.sql("DELETE FROM lakefs.test2.company.workers WHERE id = 6")
    lfs_client.commits_api.commit(repo_name, "test2", CommitCreation(message="delete one row"))
    lfs_client.refs_api.merge_into_branch(repo_name, "test2", "main")
    df_main = spark.read.table("lakefs.main.company.workers")
    df_dev = spark.read.table("lakefs.test2.company.workers")
    assert (df_main.schema == df_dev.schema) and (df_main.collect() == df_dev.collect()), "main and test2 tables should be equal"


def test_multiple_changes_and_merge(spark, lfs_client: lakefs_sdk.client.LakeFSClient, lakefs_repo, storage_namespace):
    repo_name = f"{lakefs_repo}_test3"
    initiate_repo_with_data(spark, lfs_client, repo_name, storage_namespace, "test3")

    lfs_client.branches.create_branch(repo_name, BranchCreation(name="test3", source="main"))
    spark.sql("DELETE FROM lakefs.test3.company.workers WHERE id = 6")
    spark.sql("DELETE FROM lakefs.test3.company.workers WHERE id = 5")
    spark.sql("INSERT INTO lakefs.test3.company.workers VALUES (7, 'Jhon', 'Smith', 33, 'M')")
    spark.sql("DELETE FROM lakefs.test3.company.workers WHERE id = 4")
    spark.sql("INSERT INTO lakefs.test3.company.workers VALUES (8, 'Marta', 'Green', 31, 'F')")
    lfs_client.commits.commit(repo_name, "test3", CommitCreation(message="Some changes"))
    lfs_client.refs.merge_into_branch(repo_name, "test3", "main")
    df_main = spark.read.table("lakefs.main.company.workers")
    df_dev = spark.read.table("lakefs.test3.company.workers")
    assert (df_main.schema == df_dev.schema) and (df_main.collect() == df_dev.collect()), "main and dev tables should be equal"