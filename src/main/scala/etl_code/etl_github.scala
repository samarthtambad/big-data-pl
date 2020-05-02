package etl

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.avro._
import org.apache.spark.sql.types.{StructField, StructType, StringType, IntegerType, TimestampType, ShortType, DoubleType}

object TransformGithubRaw {

    // define path to data 
    val basePath: String = "project/data/"
    val rawDataPath: String = basePath + "raw/data/"
    val cleanedDataPath: String = basePath + "cleaned/"

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

    private def transformUsersData(spark: SparkSession): Unit = {
        val usersDF = spark.read.format("csv").schema(usersSchema).load(rawDataPath + "users.csv")
        val usersDF_nonull = usersDF.na.drop()      // remove null values
        val usersDF_dropped = usersDF_nonull.drop("login").drop("name").drop("type").drop("fake").drop("deleted").drop("long").drop("lat").drop("country_code").drop("company").drop("state").drop("city")
        val usersDF_cleaned = usersDF_dropped.withColumn("year", split(col("created_at"), "-")(0)).drop("created_at")   // convert timestamp to year
        
        usersDF_cleaned.write.format("avro").mode("overwrite").save(cleanedDataPath + "users.avro")
    }

    private def transformProjectsData(spark: SparkSession): Unit = {
        val projectsDF = spark.read.format("csv").schema(projectsSchema).load(rawDataPath + "projects.csv")
        val projectsDF_nonull = projectsDF.na.drop()    // remove null values
        val projectsDF_dropped = projectsDF_nonull.drop("url").drop("name").drop("descriptor").drop("forked_from").drop("deleted").drop("updated_at")
        val projectsDF_cleaned = projectsDF_dropped.filter(!projectsDF_dropped("language").contains("\\N")).withColumn("year", split(col("created_at"), "-")(0)).drop("created_at").withColumn("language", lower(col("language")))
        
        projectsDF_cleaned.write.format("avro").mode("overwrite").save(cleanedDataPath + "projects.avro")
    }

    private def transformProjectLangData(spark: SparkSession): Unit = {
        val projectLanguagesDF = spark.read.format("csv").schema(projectLanguagesSchema).load(rawDataPath + "project_languages.csv")
        val projectLanguagesDF_nonull = projectLanguagesDF.na.drop()    // remove null values
        val projectLanguagesDF_dropped = projectLanguagesDF_nonull.drop("bytes")    // drop unwanted columns
        val projectLanguagesDF_cleaned = projectLanguagesDF_dropped.withColumn("year", split(col("created_at"), "-")(0)).drop("created_at")
        
        projectLanguagesDF_cleaned.write.format("avro").mode("overwrite").save(cleanedDataPath + "project_languages.avro")
    }

    private def transformPullRequestData(spark: SparkSession): Unit = {
        val pullRequestsDF = spark.read.format("csv").schema(pullRequestsSchema).load(rawDataPath + "pull_requests.csv")
        val pullRequestsDF_nonull = pullRequestsDF.na.drop()    // remove null values
        val pullRequestsDF_dropped = pullRequestsDF_nonull.drop("intra_branch")     // drop unwanted columns
        val pullRequestsDF_cleaned = pullRequestsDF_dropped
        
        pullRequestsDF_cleaned.write.format("avro").mode("overwrite").save(cleanedDataPath + "pull_requests.avro")
    }

    private def transformCommitsData(spark: SparkSession): Unit = {
        val commitsDF = spark.read.format("csv").schema(commitsSchema).load(rawDataPath + "commits.csv")
        val commitsDF_nonull = commitsDF.na.drop()      // remove null values
        val commitsDF_dropped = commitsDF_nonull.drop("sha")    // drop unwanted columns    
        val commitsDF_cleaned = commitsDF_dropped.withColumn("year", split(col("created_at"), "-")(0)).drop("created_at")
        
        commitsDF_cleaned.write.format("avro").mode("overwrite").save(cleanedDataPath + "commits.avro")
    }

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder
        .appName("TransformGithubRaw")
        .getOrCreate()
        
        transformUsersData(spark)
        transformProjectsData(spark)
        transformProjectLangData(spark)
        transformPullRequestData(spark)
        transformCommitsData(spark)
        
    }

}
