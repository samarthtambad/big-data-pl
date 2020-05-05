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
        StructField("year", IntegerType, false)
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

    val issuesSchema = StructType(Array(
        StructField("id", IntegerType, false),
        StructField("repo_id", IntegerType, false),
        StructField("issue_id", IntegerType, false),
        StructField("year", IntegerType, false)
    ))

    val issueEventsSchema = StructType(Array(
        StructField("event_id", IntegerType, false),
        StructField("issue_id", IntegerType, false),
        StructField("action", StringType, false),
        StructField("year", IntegerType, false)
    ))

    val metricsSchema = StructType(Array(
        StructField("year", IntegerType, false),
        StructField("language", StringType, false),
        StructField("metric", IntegerType, false)
    ))

    val languagesSchema = StructType(Array(
        StructField("language", StringType, false)
    ))

    // generate a list of top 50 languages to limit the scope
    private def computeLanguageList(spark: SparkSession, outFileName: String): Unit = {
        val projectsDF = spark.read.format("csv").schema(projectsSchema).load(basePath + "projects.csv")
        val languagesDF = projectsDF.groupBy("language").agg(count("id") as "count").sort(desc("count")).drop("count")
        languagesDF.limit(50).coalesce(1).write.format("csv").mode("overwrite").save(baseSavePath + outFileName) 
    }

    // save number of projects per language per year
    private def computeNumProjects(spark: SparkSession, outFileName: String, projectsDF: DataFrame): Unit = {
        val languagesDF = spark.read.format("csv").schema(languagesSchema).load(basePath + "languages_list.csv")
        val numProjectsDF = projectsDF.groupBy("year", "language").agg(count("language") as "num_projects").sort(desc("num_projects"))  // rows = 577
        
        // save computed data to hdfs
        numProjectsDF.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(baseSavePath + outFileName)
    }

    // save number of commits per language per year
    private def computeNumCommits(spark: SparkSession, outFileName: String, projectsDF: DataFrame): Unit = {
        val languagesDF = spark.read.format("csv").schema(languagesSchema).load(basePath + "languages_list.csv")
        val commitsDF = spark.read.format("csv").schema(commitsSchema).load(basePath + "commits.csv").drop("author_id").drop("committer_id")
        
        commitsDF.cache()
        projectsDF.cache()

        // there are many commits per project. reduce by number of commits per project
        // this command reduces the number of rows by a factor of 100. (necessary as next step is a join)
        val commitsReducedDF = commitsDF.groupBy("year", "project_id").agg(count("id") as "num_commits")

        // join commits and project_languages by project_id. 
        val joinedDF = commitsReducedDF.join(projectsDF, "project_id")
        val numCommitsProjectDF = joinedDF.groupBy("project_id", "year", "language").agg(sum("num_commits") as "num_commits")

        val numCommitsDF = numCommitsProjectDF.groupBy("year", "language").agg(sum("num_commits") as "num_commits").sort(desc("num_commits"))   // rows = 630

        // save computed data to hdfs
        numCommitsDF.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(baseSavePath + outFileName)
    }

    // save number of users per language per year
    private def computeNumUsers(spark: SparkSession, outFileName: String, projectsDF: DataFrame): Unit = {
        val commitsDF = spark.read.format("csv").schema(commitsSchema).load(basePath + "commits.csv").drop("id").drop("committer_id").withColumn("user_id", col("author_id")).drop("author_id")
        val pullRequestHistoryDF = spark.read.format("csv").schema(pullRequestsHistorySchema).load(basePath + "pull_request_history.csv").drop("id").withColumn("user_id", col("actor_id")).drop("actor_id")
        val pullRequestsDF = spark.read.format("csv").schema(pullRequestsSchema).load(basePath + "pull_requests.csv").drop("head_repo_id").drop("head_commit_id").drop("base_commit_id").drop("pull_request_id").withColumn("pull_request_id", col("id")).drop("id").withColumn("project_id", col("base_repo_id")).drop("base_repo_id")
        
        commitsDF.cache()
        projectsDF.cache()
        pullRequestsDF.cache()
        pullRequestHistoryDF.cache()

        // join pull_request_history with pull_requests to link user_id with project_id for open pull request events
        val pullRequestHistoryDF_filtered = pullRequestHistoryDF.filter(pullRequestHistoryDF("action") === "opened").drop("action")
        val pullRequestJoinedDF = pullRequestHistoryDF_filtered.join(pullRequestsDF, "pull_request_id").drop("pull_request_id")
        val pullRequestJoinedDF_reduced = pullRequestJoinedDF.distinct().select("year", "user_id", "project_id")    // rows = 16892023, cols = (year, user_id, project_id)

        // unique (user_id, project_id, year)  rows
        val commitsDF_reduced = commitsDF.distinct().select("year", "user_id", "project_id")    // rows = 116977310, cols = (year, user_id, project_id)

        // combine both commits and pull requests df using union
        val userActivityDF = pullRequestJoinedDF_reduced.union(commitsDF_reduced)   // rows = 133869333, cols = (year, user_id, project_id)

        // reduce to only unique values
        val userActivityDF_reduced = userActivityDF.distinct()  // rows = 124338394, cols = (year, user_id, project_id)

        // join with projects to link user activity to a language
        val userLanguageActivityDF = userActivityDF_reduced.join(projectsDF, "project_id").drop("project_id")   // rows = 66318766, cols = (year, user_id, language)

        // reduce to only unique rows of the form (user_id, language, year)
        val userLanguageActivityDF_reduced = userLanguageActivityDF.distinct()  // rows = 31074191

        // aggregate count of num_users per language per year
        val numUsers = userLanguageActivityDF_reduced.groupBy("year", "language").agg(count("user_id") as "num_users").sort(desc("num_users"))  // rows = 630, cols = (year, language, num_users)

        // save computed data to hdfs
        numUsers.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(baseSavePath + outFileName)
    }

    // save number of pull requests per language per year
    private def computeNumPullRequests(spark: SparkSession, outFileName: String, projectsDF: DataFrame): Unit = {
        val languagesDF = spark.read.format("csv").schema(languagesSchema).load(basePath + "languages_list.csv")
        val pullRequestsDF = spark.read.format("csv").schema(pullRequestsSchema).load(basePath + "pull_requests.csv").drop("head_repo_id").drop("head_commit_id").drop("base_commit_id").drop("pull_request_id").withColumn("pull_request_id", col("id")).drop("id").withColumn("project_id", col("base_repo_id")).drop("base_repo_id")
        val pullRequestHistoryDF = spark.read.format("csv").schema(pullRequestsHistorySchema).load(basePath + "pull_request_history.csv")
        
        pullRequestsDF.cache()
        pullRequestHistoryDF.cache()
        projectsDF.cache()

        val pullRequestHistoryDF_filtered = pullRequestHistoryDF.filter(pullRequestHistoryDF("action") === "opened").drop("action")  // only looking at pull request open event
        val prJoinedDF = pullRequestHistoryDF_filtered.join(pullRequestsDF, "pull_request_id")
        val joinedDF = prJoinedDF.join(projectsDF, "project_id")
        val numPullRequest = joinedDF.groupBy("year", "language").agg(count("id") as "num_pull_requests").sort(desc("num_pull_requests"))   // rows = 478

        // save computed data to hdfs
        numPullRequest.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(baseSavePath + outFileName)   
    }

    // save number of pending issues (not yet closed) per language per year
    private def computeNumPendingIssues(spark: SparkSession, outFileName: String, projectsDF: DataFrame): Unit = {
        val languagesDF = spark.read.format("csv").schema(languagesSchema).load(basePath + "languages_list.csv")
        val issuesDF = spark.read.format("csv").schema(issuesSchema).load(basePath + "issues.csv").drop("issue_id")
            .withColumn("issue_id", col("id")).drop("id").withColumn("project_id", col("repo_id")).drop("repo_id")
            .filter(col("year") >= 2007 && col("year") <= 2019)
        val issueEventsDF = spark.read.format("csv").schema(issueEventsSchema).load(basePath + "issue_events.csv")
            .drop("year").filter(col("year") >= 2007 && col("year") <= 2019)

        issuesDF.cache()
        projectsDF.cache()
        issueEventsDF.cache()

        // filter only issue close events
        val issueEventsDF_filtered = issueEventsDF.filter(issueEventsDF("action") === "closed").drop("action")

        // remove rows in issues that match issue_id in issue_events
        val pendingIssuesDF = issuesDF.join(issueEventsDF_filtered, Seq("issue_id"), "left_anti")

        // join with project to link issue with language
        val joinedPendingIssuesDF = pendingIssuesDF.join(projectsDF, "project_id").drop("project_id")
        val joinedPendingIssuesDF_distinct = joinedPendingIssuesDF.distinct()
        val numPendingIssues = joinedPendingIssuesDF_distinct.groupBy("year", "language").agg(count("issue_id") as "num_pending_issues").sort(desc("num_pending_issues"))   // rows = 527

        // save computed data to hdfs
        numPendingIssues.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(baseSavePath + outFileName) 
    }

    // join individual metrics computed in seperate functions into one df
    // grouped by year and programming language
    private def computeFinalMetrics(spark: SparkSession, outFileName: String): Unit = {
        val numProjectsDF = spark.read.format("csv").option("header", "true").schema(metricsSchema).load(baseSavePath + "time_num_projects.csv").withColumn("num_projects", col("metric")).drop("metric")
        val numCommitsDF = spark.read.format("csv").option("header", "true").schema(metricsSchema).load(baseSavePath + "time_num_commits.csv").withColumn("num_commits", col("metric")).drop("metric").na.drop()
        val numUsersDF = spark.read.format("csv").option("header", "true").schema(metricsSchema).load(baseSavePath + "time_num_users.csv").withColumn("num_users", col("metric")).drop("metric")
        val numPullReqDF = spark.read.format("csv").option("header", "true").schema(metricsSchema).load(baseSavePath + "time_num_pull_req.csv").withColumn("num_pull_req", col("metric")).drop("metric")
        val numPendingIssuesDF = spark.read.format("csv").option("header", "true").schema(metricsSchema).load(baseSavePath + "time_num_pending_issues.csv").withColumn("num_pending_issues", col("metric")).drop("metric")

        // join all df by (year, language)
        val joinedDF1 = numProjectsDF.join(numCommitsDF, Seq("year", "language"), "outer")
        val joinedDF2 = joinedDF1.join(numUsersDF, Seq("year", "language"), "outer")
        val joinedDF3 = joinedDF2.join(numPullReqDF, Seq("year", "language"), "outer")
        val finalMetricsDF = joinedDF3.join(numPendingIssuesDF, Seq("year", "language"), "outer")

        // save computed data to hdfs
        finalMetricsDF.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(baseSavePath + outFileName)
    }

    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder.appName("AnalyzeGithub").getOrCreate()

        // restrict to only top 50 popular languages
        val languages = spark.sparkContext.textFile(basePath + "languages_list.csv")
        val languages_array = languages.collect().toList
        val projectsDF = spark.read.format("csv").schema(projectsSchema).load(basePath + "projects.csv")
            .drop("owner_id").drop("year").withColumn("project_id", col("id")).drop("id").filter(col("language")
            .isin(languages_array:_*))
        val projectsDF_withYear = spark.read.format("csv").schema(projectsSchema).load(basePath + "projects.csv")
            .drop("owner_id").withColumn("project_id", col("id")).drop("id").filter(col("language")
            .isin(languages_array:_*))

        // compute metrics
        computeLanguageList(spark, "languages_list.csv")
        computeNumProjects(spark, "time_num_projects.csv", projectsDF_withYear)
        computeNumCommits(spark, "time_num_commits.csv", projectsDF)
        computeNumUsers(spark, "time_num_users.csv", projectsDF)
        computeNumPullRequests(spark, "time_num_pull_req.csv", projectsDF)
        computeNumPendingIssues(spark, "time_num_pending_issues.csv", projectsDF)
        computeFinalMetrics(spark, "github_final_metrics.csv")
    }

}