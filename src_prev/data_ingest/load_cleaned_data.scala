import org.apache.spark.sql.types.{StructField, StructType, StringType, IntegerType, TimestampType, ShortType, DoubleType}

val data_path: String = "project/data/test/cleaned/"

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

val usersDF = spark.read.format("csv").schema(usersSchema).load(data_path + "users.csv")
val commitsDF = spark.read.format("csv").schema(commitsSchema).load(data_path + "commits.csv")
val pullRequestsDF = spark.read.format("csv").schema(pullRequestsSchema).load(data_path + "pull_requests.csv")
val projectsDF = spark.read.format("csv").schema(projectsSchema).load(data_path + "projects.csv")
val projectLanguagesDF = spark.read.format("csv").schema(projectLanguagesSchema).load(data_path + "project_languages.csv")
