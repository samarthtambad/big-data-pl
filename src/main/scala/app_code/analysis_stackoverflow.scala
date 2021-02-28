package analysis

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, StructType, StringType, IntegerType, LongType, TimestampType, ShortType, DoubleType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame

    /* 
     *  Data: StackOverflow
     *  Source: https://archive.org/details/stackexchange
     *  Size: 16GB
     *  Schema: https://meta.stackexchange.com/questions/2677/database-schema-documentation-for-the-public-data-dump-and-sede
    */
object AnalyzeStackOverflow {

    // define path to data 
    val basePath: String = "project/data/cleaned/"
    val baseSavePath: String = "project/data/analysis/"

    val postsSchema = StructType(Array(
        StructField("_AnswerCount", IntegerType, true), 
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
        var df = spark.read.format("csv").schema(postsSchema).load(basePath + "posts.csv/*")

        df = df.withColumnRenamed("_CreationYear","year")
        df = df.withColumnRenamed("_Tag","language")
        df = df.withColumn("response_time", datediff(df("_ClosedDate"), df("_CreationDate"))/3600)
        
        val languages = spark.sparkContext.textFile("/user/svt258/project/data/cleaned/" + "languages_list.csv")
        val languages_array = languages.collect().toList
        df = df.filter(col("language").isin(languages_array:_*))
        
        df.cache()

        val questionsDF = df.filter(df("_PostTypeId") === 1)
        //val answersDF = df.filter(df("_PostTypeId") === 2)
        val numberOfQuestions = questionsDF.groupBy("year", "language").agg(count("year") as "so_num_questions").sort(desc("so_num_questions"))
        val numberOfAnswers = df.groupBy("year", "language").agg(sum("_AnswerCount") as "so_num_answers").sort(desc("so_num_answers"))
        
        val numberOfUsers = df.groupBy("year", "language").agg(countDistinct("_OwnerUserId") as "so_num_users").sort(desc("so_num_users"))

        val totalScore = df.groupBy("year", "language").agg(sum("_Score") as "so_total_score").sort(desc("so_total_score"))

        //val unansweredQuestionsDF = questionsDF.filter(questionsDF.col("_ClosedDate").isNull)
        //val unansweredQuestions = df.groupBy("year", "language").agg(count("year") as "so_unanswered_questions").sort(desc("so_unanswered_questions"))

        val unansweredQuestionsDF = questionsDF.filter(questionsDF("_AnswerCount" === 0))
        val unansweredQuestions = df.groupBy("year", "language").agg(count("year") as "so_unanswered_questions").sort(desc("so_unanswered_questions"))

        val averageResponseTime = df.groupBy("year", "language").agg(avg("response_time") as "so_avg_response_time").sort(desc("so_avg_response_time"))

        numberOfQuestions.cache()
        numberOfAnswers.cache()
        numberOfUsers.cache()
        totalScore.cache()
        unansweredQuestions.cache()
        averageResponseTime.cache()

        // join all df by (year, language)
        val joinedDF1 = numberOfQuestions.join(numberOfAnswers, Seq("year", "language"), "outer")
        val joinedDF2 = joinedDF1.join(numberOfUsers, Seq("year", "language"), "outer")
        val joinedDF3 = joinedDF2.join(totalScore, Seq("year", "language"), "outer")
        val joinedDF4 = joinedDF3.join(unansweredQuestions, Seq("year", "language"), "outer")
        val finalMetricsDF = joinedDF4.join(averageResponseTime, Seq("year", "language"), "outer")

        // save computed data to hdfs
        finalMetricsDF.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(baseSavePath + outFileName)
    }

    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder.appName("AnalyzeStackOverflow").getOrCreate()
        computeFinalMetrics(spark, "stackoverflow_final_metrics.csv")
    }

}

