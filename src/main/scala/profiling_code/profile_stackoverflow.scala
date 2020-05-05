package profile

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, StructType, StringType, IntegerType, LongType, TimestampType, ShortType, DoubleType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame

object ProfileStackOverflow {
    /* 
     *  Data: StackOverflow
     *  Source: https://archive.org/details/stackexchange
     *  Size: 16GB
     *  Schema: https://meta.stackexchange.com/questions/2677/database-schema-documentation-for-the-public-data-dump-and-sede
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

    val postsSchema = StructType(Array(
        StructField("_ClosedDate", TimestampType, true), 
        StructField("_CreationDate", TimestampType, true), 
        StructField("_Id", IntegerType, true),
        StructField("_OwnerUserId", IntegerType, true), 
        StructField("_PostTypeId", IntegerType, true),
        StructField("_Score", IntegerType, true),
        StructField("_Tag", StringType, true), 
        StructField("_CreationYear", IntegerType, true), 
        StructField("_AnswerCount", IntegerType, true)
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

    def getStatsForCol(spark: SparkSession, df: DataFrame, colName: String): DataFrame = {
        val colType: String = df.schema(colName).dataType.toString
        val numRows: Long = df.count()
        val numNulls: Long = df.filter(df(colName).isNull || df(colName).isNaN).count()
        val numSpaces: Long = df.filter(df(colName) === " ").count()
        val numBlanks: Long = df.filter(df(colName) === "").count()
        val countProper: Long = numRows - numNulls - numSpaces - numBlanks
        val minMax = df.schema(colName).dataType match {
            case StringType => df.agg(min(length(col(colName))), max(length(col(colName)))).head()
            case _ => df.agg(min(colName), max(colName)).head()
        }
        val colMin: Int = minMax.getInt(0)
        val colMax: Int = minMax.getInt(1)
        val numDistinct: Long = df.agg(countDistinct(colName)).head().getLong(0)

        val newRow = Seq(Row(colName, colType, numRows, numNulls, numSpaces, numBlanks, countProper, colMin, colMax, numDistinct))
        val newRowDF = spark.createDataFrame(spark.sparkContext.parallelize(newRow), profileStatsSchema)
        return newRowDF
    }

    private def profilePostsData(spark: SparkSession): Unit = {
        val postsDF = spark.read.format("csv").schema(postsSchema).load(basePath + "posts.csv/*")
        postsDF.cache()
        
        //val p1 = getStatsForCol(spark, postsDF, "_ClosedDate")
        //val p2 = getStatsForCol(spark, postsDF, "_CreationDate")
        val p3 = getStatsForCol(spark, postsDF, "_Id")
        val p4 = getStatsForCol(spark, postsDF, "_OwnerUserId")
        val p5 = getStatsForCol(spark, postsDF, "_PostTypeId")
        val p6 = getStatsForCol(spark, postsDF, "_Score")
        val p7 = getStatsForCol(spark, postsDF, "_Tag")
        val p8 = getStatsForCol(spark, postsDF, "_CreationYear")
        val p9 = getStatsForCol(spark, postsDF, "_AnswerCount")


        val emptyDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], profileStatsSchema)
        //val df1 = emptyDF.union(p1)
        //val df2 = df1.union(p2)
        val df3 = emptyDF.union(p3)
        val df4 = df3.union(p4)
        val df5 = df4.union(p5)
        val df6 = df5.union(p6)
        val df7 = df6.union(p7)
        val df8 = df6.union(p8)
        val finalDF = df8.union(p9)

        finalDF.coalesce(1).write.format("csv").mode("overwrite").option("header", "true").save(baseSavePath + "posts_stats.csv")
    }


    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession.builder.appName("ProfileStackOverflow").getOrCreate()

        profilePostsData(spark)
    }

}
