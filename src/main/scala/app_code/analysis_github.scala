package analysis

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, StructType, StringType, IntegerType, LongType, TimestampType, ShortType, DoubleType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame

object AnalyzeGithub {
    /* 
     *  Data: Github
     *  Source: https://ghtorrent.org/downloads.html
     *  Size: 102 GB
     *  Schema: https://ghtorrent.org/files/schema.pdf
    */

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
        joinedDF.write.format("csv").mode("overwrite").save(basePath + outFileName)
        return joinedDF
    }

    // get number of projects by language per year
    private def computeNumProjects(spark: SparkSession, df: DataFrame): Unit = {
        val numProjectsDF = df.rollup("year", "language").agg(count("language") as "count").sort(desc("count"))
        numProjectsDF.write.format("csv").mode("overwrite").save(baseSavePath + "time_num_projects.csv")
    }

    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder.appName("AnalyzeGithub").getOrCreate()

        // val usersDF = spark.read.format("csv").schema(usersSchema).load(basePath + "users.csv")
        
        val projectsDF = spark.read.format("csv").schema(projectsSchema).load(basePath + "projects.csv")
        projectsDF.cache()

        // val projectLanguagesDF = spark.read.format("csv").schema(projectLanguagesSchema).load(basePath + "project_languages.csv")
        // val pullRequestsDF = spark.read.format("csv").schema(pullRequestsSchema).load(basePath + "pull_requests.csv")
        // val commitsDF = spark.read.format("csv").schema(commitsSchema).load(basePath + "commits.csv")

        computeNumProjects(spark, projectsDF)

    }

}