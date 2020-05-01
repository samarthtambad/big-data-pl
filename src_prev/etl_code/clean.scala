/* 
Clean the data

Assume following DFs already loaded in interactive mode

usersDF
commitsDF
pullRequestsDF
projectsDF
projectLanguagesDF
*/

// set path to data
val data_path_cleaned: String = "project/data/test/cleaned/"

// ------------ users.csv -----------//
// remove null values
val usersDF_nonull = usersDF.na.drop()
// drop unwanted columns
val usersDF_dropped = usersDF_nonull.drop("login").drop("name").drop("type").drop("fake").drop("deleted").drop("long").drop("lat").drop("country_code").drop("company").drop("state").drop("city")
// convert timestamp to year
val usersDF_cleaned = usersDF_dropped.withColumn("year", split(col("created_at"), "-")(0)).drop("created_at")
usersDF_cleaned.coalesce(1).write.format("csv").mode("overwrite").save(data_path_cleaned + "users.csv")


// ------------ commits.csv -----------//
// remove null values
val commitsDF_nonull = commitsDF.na.drop()
// drop unwanted columns
val commitsDF_dropped = commitsDF_nonull.drop("sha")
// convert timestamp to year
val commitsDF_cleaned = commitsDF_dropped.withColumn("year", split(col("created_at"), "-")(0)).drop("created_at")
commitsDF_cleaned.coalesce(1).write.format("csv").mode("overwrite").save(data_path_cleaned + "commits.csv")


// ------------ pull_requests.csv -----------//
// remove null values
val pullRequestsDF_nonull = pullRequestsDF.na.drop()
// drop unwanted columns
val pullRequestsDF_dropped = pullRequestsDF_nonull.drop("intra_branch")
//
val pullRequestsDF_cleaned = pullRequestsDF_dropped
pullRequestsDF_cleaned.coalesce(1).write.format("csv").mode("overwrite").save(data_path_cleaned + "pull_requests.csv")

// ------------ projects.csv -----------//
// remove null values
val projectsDF_nonull = projectsDF.na.drop()
// drop unwanted columns
val projectsDF_dropped = projectsDF_nonull.drop("url").drop("name").drop("descriptor").drop("forked_from").drop("deleted").drop("updated_at")
// additional cleaning
val projectsDF_cleaned = projectsDF_dropped.filter(!projectsDF_dropped("language").contains("\\N")).withColumn("year", split(col("created_at"), "-")(0)).drop("created_at").withColumn("language", lower(col("language")))
projectsDF_cleaned.coalesce(1).write.format("csv").mode("overwrite").save(data_path_cleaned + "projects.csv")


// ------------ project_languages.csv -----------//
// remove null values
val projectLanguagesDF_nonull = projectLanguagesDF.na.drop()
// drop unwanted columns
val projectLanguagesDF_dropped = projectLanguagesDF_nonull.drop("bytes")
// additional cleaning
val projectLanguagesDF_cleaned = projectLanguagesDF_dropped.withColumn("year", split(col("created_at"), "-")(0)).drop("created_at")
projectLanguagesDF_cleaned.coalesce(1).write.format("csv").mode("overwrite").save(data_path_cleaned + "project_languages.csv")