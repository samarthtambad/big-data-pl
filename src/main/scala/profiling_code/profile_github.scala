package profile

import org.apache.spark.sql.SparkSession
// import com.databricks.spark.avro._
import org.apache.spark.sql.types.{StructField, StructType, StringType, IntegerType, TimestampType, ShortType, DoubleType}
import org.apache.spark.sql.functions.lower

object ProfileGithub {
    /* 
    Github data
    Source: https://ghtorrent.org/downloads.html
    Size: 102 GB
    Schema: https://ghtorrent.org/files/schema.pdf
    */

    // define path to data
    val basePath: String = "project/data/cleaned/"

    /*
        DataFrame: 
        1. Column Count
        2. Row Count

        Integer data columns:
        1. Max, Min
        2. Distinct

        String data columns:
        1. MaxLen, MinLen
        2. Distinct

    */

    private def profileUsersData(spark: SparkSession): Unit = {
        // id (Integer)
        // year (Integer)
        val usersDF = spark.read.format("avro").load(basePath + "users.avro")

        val colCount = usersDF.columns.size
        val rowCount = usersDF.count()        

    }

    private def profileCommitsData(spark: SparkSession): Unit = {
        val commitsDF = spark.read.format("avro").load(basePath + "commits.avro")

    }

    private def profilePullRequestsData(spark: SparkSession): Unit = {
        val pullRequestsDF = spark.read.format("avro").load(basePath + "pull_requests.avro")
    
    }

    private def profileProjectsData(spark: SparkSession): Unit = {
        val projectsDF = spark.read.format("avro").load(basePath + "projects.avro")
    
    }

    private def profileProjectLangData(spark: SparkSession): Unit = {
        val projectLanguagesDF = spark.read.format("avro").load(basePath + "project_languages.avro")

    }

    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder.appName("ProfileGithub").getOrCreate()

        profileUsersData(spark)
        profileProjectsData(spark)
        profileProjectLangData(spark)
        profilePullRequestsData(spark)
        profileCommitsData(spark)

    }

}
