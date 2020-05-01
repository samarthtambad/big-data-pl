
// Github

// load project_languages
val projectLanguagesData = sqlContext.load("com.databricks.spark.csv", Map("path" -> "project/data/cleaned/project_languages/*"))

// load commits
val commitsDF = sqlContext.load("com.databricks.spark.csv", Map("path" -> "project/data/cleaned/commits/*"))

// join both tables with project_id
val joinedCommitsLangData = commitsDF.join(projectLanguagesData, Seq("project_id"), "inner")

// count number of commits grouped by language
val df_countCommitLangData = sqlContext.sql("""SELECT year, COUNT(language) FROM joinedCommitsLangData GROUP BY year""")

// count number of projects by language
val df_countProjectLang = sqlContext.sql("""SELECT year, COUNT(language) FROM projectLanguagesData GROUP BY year""")

// save data
df_countCommitLangData.coalesce(1).write.format("com.databricks.spark.csv").save("project/data/computed/commit_languages.csv")
df_countProjectLang.coalesce(1).write.format("com.databricks.spark.csv").save("project/data/computed/num_projects_lang.csv")




//StackOverflow
//load data using the databricks xml reader into spark sql context
spark2-shell --packages com.databricks:spark-xml_2.11:0.9.0

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType, TimestampType}
import com.databricks.spark.xml._
import org.apache.spark.sql.SQLContext

val spark = SparkSession.builder.getOrCreate()

val sqlContext = new SQLContext(sc)
import sqlContext._
import sqlContext.implicits._


CREATE TABLE SO
USING com.databricks.spark.xml
OPTIONS (path "rhn235/stackoverflow/Posts.xml", rowTag "row", rootTag "posts")


//formatting
val df_with_dates = df("CreationDate", to_date(unix_timestamp($"date", "YYYYMMDD").cast("timestamp")))


//counts
df_with_dates.registerTempTable("SO")
sqlContext.sql("""SELECT COUNT(*) FROM SO WHERE PostTypeID=1""") #count of questions
sqlContext.sql("""SELECT COUNT(*) FROM SO WHERE PostTypeID=3""") #count of answers
sqlContext.sql("""SELECT AVG(SCORE) FROM SO """).show()
sqlContext.sql("""SELECT MAX(SCORE) FROM SO """).show()
sqlContext.sql("""SELECT MIN(SCORE) FROM SO """).show()
sqlContext.sql("""SELECT MAX(CREATION_DATE) FROM SO """).show()
sqlContext.sql("""SELECT MIN(CREATION_DATE) FROM SO """).show()


//computing metrics
total_questions_by_language_year = sqlContext.sql("""SELECT COUNT(*) WHERE PostTypeID=1 FROM SO GROUP BY TAG, YEAR""")
questions_with_unanswered_questions_year = sqlContext.sql("""SELECT COUNT(*) WHERE AnswerCount = 0 FROM SO GROUP BY TAG, YEAR""")
total_score_by_language_year = sqlContext.sql("""SELECT SUM(SCORE) FROM SO GROUP BY TAG, YEAR""")
average_question_response_time = sqlContext.sql("SELECT AVG()")



