package profile

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, StructType, StringType, IntegerType, LongType, TimestampType, ShortType, DoubleType}
import org.apache.spark.sql.functions.lower
import org.apache.spark.sql.functions.countDistinct
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame

object ProfileGithub {
    /* 
     *  Data: Github
     *  Source: https://ghtorrent.org/downloads.html
     *  Size: 102 GB
     *  Schema: https://ghtorrent.org/files/schema.pdf
    */

    // define path to data
    val basePath: String = "project/data/cleaned/"
    val baseSavePath: String = "project/data/stats/"

    /* 
        Profile Info
        ------------
        DataFrame: 
        1. Column Count
        2. Row Count

        Integer data columns:
        1. Max, Min
        2. Distinct

        String data columns:
        1. MaxLen, MinLen
        2. Distinct Values
        3. Number of Distinct
    */

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

    // reference for design
    // https://towardsdatascience.com/profiling-big-data-in-distributed-environment-using-spark-a-pyspark-data-primer-for-machine-78c52d0ce45
    val profileStatsSchema = StructType(Array(
        StructField("col_name", StringType, false),
        StructField("data_type", StringType, false),
        StructField("num_rows", LongType, false),
        StructField("num_nulls", LongType, false),
        StructField("num_spaces", LongType, false),
        StructField("num_blanks", LongType, false),
        StructField("count", LongType, false),
        StructField("min", IntegerType, false),
        StructField("max", IntegerType, false),
        StructField("num_distinct", LongType, false)
    ))

    // val profileStatsDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], profileStatsSchema)
    // val newRow = Seq(Row("int", 1000, 100, 60, 40, 800, 20, 10, 10))
    // val newRowDF = spark.createDataFrame(spark.sparkContext.parallelize(newRow), profileStatsSchema)
    // val appendedDF = profileStatsDF.union(newRowDF)

    def getStatsIntegerCol(df: DataFrame, colName: String): DataFrame = {

        val colType: String = df.schema(colName).dataType.toString
        val numRows: Long = df.count()
        val numNulls: Long = df.filter(df(colName).isNull || df(colName).isNaN).count()
        val numSpaces: Long = df.filter(df(colName) === " ").count()
        val numBlanks: Long = df.filter(df(colName) === "").count()
        val countProper: Long = numRows - numNulls - numSpaces - numBlanks
        val minMax = df.agg(min(colName), max(colName)).head()
        val colMin: Int = minMax.getInt(0)
        val colMax: Int = minMax.getInt(1)
        val numDistinct: Long = df.agg(countDistinct(colName)).head().getLong(0)

        val newRow = Seq(Row(colName, colType, numRows, numNulls, numSpaces, numBlanks, countProper, colMin, colMax, numDistinct))
        val newRowDF = spark.createDataFrame(spark.sparkContext.parallelize(newRow), profileStatsSchema)
        return newRowDF
    }

    private def profileUsersData(spark: SparkSession): Unit = {
        val usersDF = spark.read.format("csv").schema(usersSchema).load(basePath + "users.csv")
        val statsDF = getStatsIntegerCol(usersDF, "year")

    }

    private def profileCommitsData(spark: SparkSession): Unit = {
        val commitsDF = spark.read.format("csv").schema(commitsSchema).load(basePath + "commits.csv")

    }

    private def profilePullRequestsData(spark: SparkSession): Unit = {
        val pullRequestsDF = spark.read.format("csv").schema(pullRequestsSchema).load(basePath + "pull_requests.csv")
    
    }

    private def profileProjectsData(spark: SparkSession): Unit = {
        val projectsDF = spark.read.format("csv").schema(projectsSchema).load(basePath + "projects.csv")
    
    }

    private def profileProjectLangData(spark: SparkSession): Unit = {
        val projectLanguagesDF = spark.read.format("csv").schema(projectLanguagesSchema).load(basePath + "project_languages.csv")

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
