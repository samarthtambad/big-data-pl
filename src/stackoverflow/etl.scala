import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType}
import com.databricks.spark.xml._

val spark = SparkSession.builder.getOrCreate()


val customSchema = StructType(Array(
  StructField("AcceptedAnswerId", StringType, nullable = true),
  StructField("AnswerCount", StringType, nullable = true),
  StructField("Body", StringType, nullable = true),
  StructField("CreationDate", StringType, nullable = true),
  StructField("Score", DoubleType, nullable = true),
  StructField("Body", StringType, nullable = true),
  StructField("OwnerUserId", StringType, nullable = true),
  StructField("LastEditorDisplayName", DoubleType, nullable = true),
  StructField("LastEditDate", StringType, nullable = true),
  StructField("LastActivityDate", StringType, nullable = true), 
  StructField("CommentCount", StringType, nullable = true)))


 |-- _AcceptedAnswerId: long (nullable = true)
 |-- _AnswerCount: long (nullable = true)
 |-- _Body: string (nullable = true)
 |-- _ClosedDate: string (nullable = true)
 |-- _CommentCount: long (nullable = true)
 |-- _CommunityOwnedDate: string (nullable = true)
 |-- _CreationDate: string (nullable = true)
 |-- _FavoriteCount: long (nullable = true)
 |-- _Id: long (nullable = true)
 |-- _LastActivityDate: string (nullable = true)
 |-- _LastEditDate: string (nullable = true)
 |-- _LastEditorDisplayName: string (nullable = true)
 |-- _LastEditorUserId: long (nullable = true)
 |-- _OwnerDisplayName: string (nullable = true)
 |-- _OwnerUserId: long (nullable = true)
 |-- _ParentId: long (nullable = true)
 |-- _PostTypeId: long (nullable = true)
 |-- _Score: long (nullable = true)
 |-- _Tags: string (nullable = true)
 |-- _Title: string (nullable = true)
 |-- _ViewCount: long (nullable = true)

val df = spark.read.option("rootTag", "posts").option("rowTag", "row").schema(customSchema).xml("rhn235/stackoverflow/Posts_small.xml")


val customSchema = StructType(Array(StructField("Id", StringType, true)))

val df = spark.read.xml("rhn235/stackoverflow/Posts.xml")


val selectedData = df.select("Id", "Body")
selectedData.write.xml("text.xml")