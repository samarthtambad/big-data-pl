package analysis

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, StructType, StringType, IntegerType, LongType, TimestampType, ShortType, DoubleType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame

/** 
  *  Data: Github
  *  Source: https://ghtorrent.org/downloads.html
  *  Size: 102 GB
  *  Schema: https://ghtorrent.org/files/schema.pdf
  */
object AnalyzeGithub {

    // define path to data 
    val basePath: String = "project/data/cleaned/"
    val baseSavePath: String = "project/data/analysis/"

    val usersSchema = StructType(Array(
        StructField("id", IntegerType, false),
        StructField("year", IntegerType, false)
    ))

    val commitsSchema = StructType(Array(
        StructField("id", IntegerType, false),
        StructField("author_id", IntegerType, false),
        StructField("committer_id", IntegerType, false),
        StructField("project_id", IntegerType, false),
        StructField("year", IntegerType, false)
    ))

    val pullRequestsSchema = StructType(Array(
        StructField("id", IntegerType, false),
        StructField("head_repo_id", IntegerType, false),
        StructField("base_repo_id", IntegerType, false),
        StructField("head_commit_id", IntegerType, false),
        StructField("base_commit_id", IntegerType, false),
        StructField("pull_request_id", IntegerType, false)
    ))

    val projectsSchema = StructType(Array(
        StructField("id", IntegerType, false),
        StructField("owner_id", IntegerType, false),
        StructField("language", StringType, false),
        StructField("year", IntegerType, false)
    ))

    val projectLanguagesSchema = StructType(Array(
        StructField("project_id", IntegerType, false),
        StructField("language", StringType, false),
        StructField("year", IntegerType, false)
    ))

    private def joinAndSave(spark: SparkSession, df1: DataFrame, df2: DataFrame, colName: String, outFileName: String): DataFrame = {
        val joinedDF = df1.join(df2, colName)
        joinedDF.write.format("csv").mode("overwrite").save(baseSavePath + outFileName)
        return joinedDF
    }

    // save number of projects per language per year
    private def computeNumProjects(spark: SparkSession, outFileName: String): Unit = {
        val projectsDF = spark.read.format("csv").schema(projectsSchema).load(basePath + "projects.csv")
        val numProjectsDF = projectsDF.groupBy("year", "language").agg(count("language") as "num_projects").sort(desc("num_projects"))
        numProjectsDF.show(10)
        
        // save computed data to hdfs
        numProjectsDF.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(baseSavePath + outFileName)
    }

    // save number of commits per language per year
    private def computeNumCommits(spark: SparkSession, outFileName: String): Unit = {
        val commitsDF = spark.read.format("csv").schema(commitsSchema).load(basePath + "commits.csv").drop("author_id").drop("committer_id")
        val projectLanguagesDF = spark.read.format("csv").schema(projectLanguagesSchema).load(basePath + "project_languages.csv").drop("year")

        val commitsReducedDF = commitsDF.groupBy("year", "project_id").agg(count("id") as "num_commits")
        commitsReducedDF.write.format("csv").mode("overwrite").option("header", "true").save(baseSavePath + "commits_reduced.csv")
        commitsReducedDF.show(10)

        val joinedDF = commitsReducedDF.join(projectLanguagesDF, "project_id")
        joinedDF.write.format("csv").mode("overwrite").option("header", "true").save(baseSavePath + "join_projectlang_commits.csv")
        joinedDF.show(10)

        val numCommitsProjectDF = joinedDF.groupBy("project_id", "year", "language").agg(sum("num_commits") as "num_commits")
        numCommitsProjectDF.write.format("csv").mode("overwrite").option("header", "true").save(baseSavePath + "commits_per_project.csv")
        numCommitsProjectDF.show(10)

        val numCommitsDF = numCommitsProjectDF.groupBy("year", "language").agg(sum("num_commits") as "num_commits").sort(desc("num_commits"))
        numCommitsDF.show(10)

        // save computed data to hdfs
        numCommitsDF.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(baseSavePath + outFileName)
    }

    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder.appName("AnalyzeGithub").getOrCreate()

        // val usersDF = spark.read.format("csv").schema(usersSchema).load(basePath + "users.csv")
        // val projectsDF = spark.read.format("csv").schema(projectsSchema).load(basePath + "projects.csv")
        // val projectLanguagesDF = spark.read.format("csv").schema(projectLanguagesSchema).load(basePath + "project_languages.csv")
        // val pullRequestsDF = spark.read.format("csv").schema(pullRequestsSchema).load(basePath + "pull_requests.csv")
        // val commitsDF = spark.read.format("csv").schema(commitsSchema).load(basePath + "commits.csv")

        computeNumProjects(spark, "time_num_projects.csv")
        computeNumCommits(spark, "time_num_commits.csv")

    }

}