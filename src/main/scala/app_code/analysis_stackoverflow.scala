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
object AnalyzeStackOverflow {

    // define path to data 
    val basePath: String = "project/data/cleaned/"
    val baseSavePath: String = "project/data/analysis/"

    val postsSchema = StructType(Array(
        StructField("_ClosedDate", TimestampType, true), 
        StructField("_CreationDate", TimestampType, true), 
        StructField("_Id", IntegerType, true),
        StructField("_OwnerUserId", IntegerType, true), 
        StructField("_PostTypeId", IntegerType, true),
        StructField("_Score", IntegerType, true),
        StructField("_Tag", StringType, true), 
        StructField("_CreationYear", IntegerType, true)
    )) 
    
    // join individual metrics computed in seperate functions into one df
    // grouped by year and programming language
    private def computeFinalMetrics(spark: SparkSession, outFileName: String): Unit = {
        //read the etl file 
        var df = spark.read.format("csv").schema(postsSchema).load(basePath + "posts.csv")
        df = df.withColumn("response_time", datediff(df("_ClosedDate"), df("_CreationDate")))
        
        val questionsDF = df.filter(df("_PostTypeId") === 1)
        val answersDF = df.filter(df("_PostTypeId") === 2)
        val numberOfQuestions = questionsDF.groupBy("_CreationYear", "_Tag").agg(count("_Tag") as "num_questions").sort(desc("num_questions"))
        val numberOfAnswers = answersDF.groupBy("_CreationYear", "_Tag").agg(count("_Tag") as "num_answers").sort(desc("num_answers"))
        val numberOfUsers = df.groupBy("_CreationYear", "_Tag", "_OwnerUserId").agg(count("_OwnerUserId") as "num_users").sort(desc("num_users"))
        val totalScore = df.groupBy("_CreationYear", "_Tag").agg(sum("_Score") as "total_score").sort(desc("total_score"))

        val unansweredQuestionsDF = questionsDF.filter(questionsDF.col("_ClosedDate").isNull)
        val unansweredQuestions = df.groupBy("_CreationYear", "_Tag").agg(count("_Tag") as "unanswered_questions").sort(desc("unanswered_questions"))

        val averageResponseTime = df.groupBy("_CreationYear", "_Tag").agg(avg("response_time") as "avg_response_time").sort(desc("avg_response_time"))

        // join all df by (year, language)
        val joinedDF1 = numberOfQuestions.join(numberOfAnswers, Seq("_CreationYear", "_Tag"))
        val joinedDF2 = joinedDF1.join(numberOfUsers, Seq("_CreationYear", "_Tag"))
        val joinedDF3 = joinedDF2.join(totalScore, Seq("_CreationYear", "_Tag"))
        val joinedDF4 = joinedDF3.join(unansweredQuestions, Seq("_CreationYear", "_Tag"))
        val finalMetricsDF = joinedDF4.join(averageResponseTime, Seq("_CreationYear", "_Tag"))

        // save computed data to hdfs
        finalMetricsDF.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(baseSavePath + outFileName)
    }

    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder.appName("AnalyzeStackOverflow").getOrCreate()
)
        computeFinalMetrics(spark, "stackoverflow_final_metrics.csv")
    }

}