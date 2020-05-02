spark2-shell --packages com.databricks:spark-xml_2.11:0.9.0

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType, TimestampType}
import com.databricks.spark.xml._
import org.apache.spark.sql.functions.split


val spark = SparkSession.builder.getOrCreate()

var df = spark.read.option("rootTag", "posts").option("rowTag", "row").xml("rhn235/stackoverflow/Posts_small.xml")
df = df.withColumn("_CreationDate", col("_CreationDate").cast("timestamp"))
df = df.withColumn("_ClosedDate", col("_ClosedDate").cast("timestamp"))
df = df.withColumn("_CommunityOwnedDate", col("_CommunityOwnedDate").cast("timestamp"))
df = df.withColumn("_LastActivityDate", col("_LastActivityDate").cast("timestamp"))
df = df.withColumn("_LastEditDate", col("_LastEditDate").cast("timestamp"))
df = df.withColumn("_ClosedDate", col("_ClosedDate").cast("timestamp"))
df = df.withColumn("_ClosedDate", col("_ClosedDate").cast("timestamp"))


df = df.withColumn("_Tag", explode(split($"_Tags", "[<]")))


df = df.withColumn("_Tag", translate(col("_Tag"), ">", ""))



val languages = sc.textFile("/user/svt258/project/data/cleaned/languages.csv")

val languages_array = languages.collect().toList

df = df.filter(col("_Tag").isin(languages_array:_*))

df = df.withColumn("_CreationYear", year($"_CreationDate"))

val tagCountsByYear = df.groupby("_CreationYear", "_Tag").agg(count("_Score") as "count").sort($"count".desc)


#df structure
#col name = platform + metric
#col name year

# by programming language per year 
    # questions
    #score
    #of answers
    (#questions or #score ) / #answers
    #average response time
    #unanswered questions
    #sentiments #ignore
    #users
