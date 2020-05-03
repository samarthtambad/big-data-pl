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

    val pullRequestsHistorySchema = StructType(Array(
        StructField("id", IntegerType, false),
        StructField("pull_request_id", IntegerType, false),
        StructField("action", StringType, false),
        StructField("actor_id", IntegerType, false),
        StructField("year", TimestampType, false)
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
        
        commitsDF.cache()
        projectLanguagesDF.cache()

        // there are many commits per project. reduce by number of commits per project
        // this command reduces the number of rows by a factor of 100. (necessary as next step is a join)
        val commitsReducedDF = commitsDF.groupBy("year", "project_id").agg(count("id") as "num_commits")
        // commitsReducedDF.write.format("csv").mode("overwrite").option("header", "true").save(baseSavePath + "commits_reduced.csv")
        // commitsReducedDF.show(10)

        // join commits and project_languages by project_id. 
        val joinedDF = commitsReducedDF.join(projectLanguagesDF, "project_id")
        // joinedDF.write.format("csv").mode("overwrite").option("header", "true").save(baseSavePath + "join_projectlang_commits.csv")
        // joinedDF.show(10)

        val numCommitsProjectDF = joinedDF.groupBy("project_id", "year", "language").agg(sum("num_commits") as "num_commits")
        // numCommitsProjectDF.write.format("csv").mode("overwrite").option("header", "true").save(baseSavePath + "commits_per_project.csv")
        // numCommitsProjectDF.show(10)

        val numCommitsDF = numCommitsProjectDF.groupBy("year", "language").agg(sum("num_commits") as "num_commits").sort(desc("num_commits"))
        // numCommitsDF.show(10)

        // save computed data to hdfs
        numCommitsDF.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(baseSavePath + outFileName)
    }

    // save number of users per language per year
    private def computeNumUsers(spark: SparkSession, outFileName: String): Unit = {
        val commitsDF = spark.read.format("csv").schema(commitsSchema).load(basePath + "commits.csv").drop("author_id").drop("committer_id")
        val projectsDF = spark.read.format("csv").schema(projectsSchema).load(basePath + "projects.csv").drop("owner_id").drop("year")
        val usersDF = spark.read.format("csv").schema(usersSchema).load(basePath + "users.csv")
        
        commitsDF.cache()
        projectsDF.cache()
        usersDF.cache()

        // there are many commits per project. reduce by number of commits per project
        // this command reduces the number of rows by a factor of 100. (necessary as next step is a join)
        val commitsReducedDF = commitsDF.groupBy("year", "project_id").agg(count("id") as "num_commits")
        commitsReducedDF.write.format("csv").mode("overwrite").option("header", "true").save(baseSavePath + "commits_reduced.csv")
        commitsReducedDF.show(10)
    }

    // save number of pull requests per language per year
    private def computeNumPullRequests(spark: SparkSession, outFileName: String): Unit = {
        val pullRequestsDF = spark.read.format("csv").schema(pullRequestsSchema).load(basePath + "pull_requests.csv").drop("head_repo_id").drop("head_commit_id").drop("base_commit_id").drop("pull_request_id")
        val pullRequestHistoryDF = spark.read.format("csv").schema(pullRequestsHistorySchema).load(basePath + "pull_request_history.csv")
        val projectLanguagesDF = spark.read.format("csv").schema(projectLanguagesSchema).load(basePath + "project_languages.csv").drop("year")
        
        pullRequestsDF.cache()
        projectLanguagesDF.cache()

        val pullRequestHistoryDF_filtered = pullRequestHistoryDF.filter(col("action") == "opened").drop("action")  // only looking at pull request open event
        val prJoinedDF = pullRequestHistoryDF_filtered.join(pullRequestsDF, pullRequestHistoryDF_filtered("pull_request_id") === pullRequestsDF("id"))
        val joinedDF = prJoinedDF.join(projectLanguagesDF, prJoinedDF("base_repo_id") === projectLanguagesDF("project_id"))
        val numPullRequest = joinedDF.groupBy("year", "language").agg(count("id") as "num_pull_requests").sort(desc("num_pull_requests"))

        // save computed data to hdfs
        numPullRequest.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(baseSavePath + outFileName)   
    }

    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder.appName("AnalyzeGithub").getOrCreate()

        // val usersDF = spark.read.format("csv").schema(usersSchema).load(basePath + "users.csv")
        // val projectsDF = spark.read.format("csv").schema(projectsSchema).load(basePath + "projects.csv")
        // val projectLanguagesDF = spark.read.format("csv").schema(projectLanguagesSchema).load(basePath + "project_languages.csv")
        // val pullRequestsDF = spark.read.format("csv").schema(pullRequestsSchema).load(basePath + "pull_requests.csv")
        // val pullRequestHistoryDF = spark.read.format("csv").schema(pullRequestsHistorySchema).load(basePath + "pull_request_history.csv")
        // val commitsDF = spark.read.format("csv").schema(commitsSchema).load(basePath + "commits.csv")

        // computeNumProjects(spark, "time_num_projects.csv")
        // computeNumCommits(spark, "time_num_commits.csv")
        // computeNumUsers(spark, "time_num_users.csv")
        computeNumPullRequests(spark, "time_num_pull_req.csv")

    }

}