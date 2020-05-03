package etl

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType, TimestampType}
import com.databricks.spark.xml._
//import org.apache.spark.sql.functions.split
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame

object TransformStackOverflowRaw {
    /* 
     *  Data: StackOverflow
     *  Source: https://archive.org/details/stackexchange
     *  Size: 16GB
     *  Schema: https://meta.stackexchange.com/questions/2677/database-schema-documentation-for-the-public-data-dump-and-sede
    */

    // define path to data 
    val basePath: String = "project/data/"
    val rawDataPath: String = basePath + "raw/data/"
    val cleanedDataPath: String = basePath + "cleaned/"


    private def transformPostsData(spark: SparkSession): Unit = {
        //Load data
        var df = spark.read.option("rootTag", "posts").option("rowTag", "row").load(rawDataPath + "Posts_small.xml")
        
        //Cast types
        df = df.withColumn("_CreationDate", col("_CreationDate").cast("timestamp"))
        df = df.withColumn("_ClosedDate", col("_ClosedDate").cast("timestamp"))
        df = df.withColumn("_CommunityOwnedDate", col("_CommunityOwnedDate").cast("timestamp"))
        df = df.withColumn("_LastActivityDate", col("_LastActivityDate").cast("timestamp"))
        df = df.withColumn("_LastEditDate", col("_LastEditDate").cast("timestamp"))
        df = df.withColumn("_ClosedDate", col("_ClosedDate").cast("timestamp"))
        df = df.withColumn("_ClosedDate", col("_ClosedDate").cast("timestamp"))

        //Clean tags
	import spark.implicits._ 
        df = df.withColumn("_Tag", explode(split($"_Tags", "[<]")))
        df = df.withColumn("_Tag", translate(col("_Tag"), ">", ""))

        //Get languages list
        val languages = spark.sparkContext.textFile("/user/svt258/project/data/cleaned/languages.csv")
        val languages_array = languages.collect().toList

        //Filter tags based on language
        df = df.filter(col("_Tag").isin(languages_array:_*))

        //Get creation year
        df = df.withColumn("_CreationYear", year($"_CreationDate"))

        //Save cleaned data
        df.write.format("csv").mode("overwrite").save(cleanedDataPath + "posts.csv")
    }

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder
        .appName("TransformStackOverflowRaw")
        .getOrCreate()
        
        transformPostsData(spark)
    }
}
