package ingest

import org.apache.spark.sql.types.{StructField, StructType, StringType, IntegerType, TimestampType, ShortType, DoubleType}

class LoadRaw(var path: String) {

    // set path to data, toggle small/big
    // val data_path: String = "project/data/raw/data/"
    val data_path: String = "project/data/test/"

    // define schema
    val usersSchema = StructType(Array(
        StructField("id", IntegerType, false),
        StructField("login", StringType, false),
        StructField("name", StringType, true),
        StructField("created_at", TimestampType, false),
        StructField("type", StringType, false),
        StructField("fake", ShortType, false),
        StructField("deleted", ShortType, false),
        StructField("long", DoubleType, true),
        StructField("lat", DoubleType, true),
        StructField("country_code", StringType, true),
        StructField("company", StringType, true),
        StructField("state", StringType, true),
        StructField("city", StringType, true)
    ))

    val commitsSchema = StructType(Array(
        StructField("id", IntegerType, false),
        StructField("sha", StringType, false),
        StructField("author_id", IntegerType, false),
        StructField("committer_id", IntegerType, false),
        StructField("project_id", IntegerType, false),
        StructField("created_at", TimestampType, false)
    ))

    val pullRequestsSchema = StructType(Array(
        StructField("id", IntegerType, false),
        StructField("head_repo_id", IntegerType, false),
        StructField("base_repo_id", IntegerType, false),
        StructField("head_commit_id", IntegerType, false),
        StructField("base_commit_id", IntegerType, false),
        StructField("pull_request_id", IntegerType, false),
        StructField("intra_branch", ShortType, true)
    ))

    val projectsSchema = StructType(Array(
        StructField("id", IntegerType, false),
        StructField("url", StringType, false),
        StructField("owner_id", IntegerType, false),
        StructField("name", StringType, false),
        StructField("descriptor", StringType, false),
        StructField("language", StringType, false),
        StructField("created_at", TimestampType, false),
        StructField("forked_from", IntegerType, false),
        StructField("deleted", ShortType, false),
        StructField("updated_at", TimestampType, false)
    ))

    val projectLanguagesSchema = StructType(Array(
        StructField("project_id", IntegerType, false),
        StructField("language", StringType, false),
        StructField("bytes", IntegerType, true),
        StructField("created_at", TimestampType, false)
    ))

    def loadData() {
        // load data
        val usersDF = spark.read.format("csv").schema(usersSchema).load(data_path + "users.csv")
        val commitsDF = spark.read.format("csv").schema(commitsSchema).load(data_path + "commits.csv")
        val pullRequestsDF = spark.read.format("csv").schema(pullRequestsSchema).load(data_path + "pull_requests.csv")
        val projectsDF = spark.read.format("csv").schema(projectsSchema).load(data_path + "projects.csv")
        val projectLanguagesDF = spark.read.format("csv").schema(projectLanguagesSchema).load(data_path + "project_languages.csv")
    }

}
