/*
generate subset of data for testing
*/

// set path to data
val data_path_small: String = "project/data/test/"

// take subset
val usersSmallDF = usersDF.limit(1000)
usersSmallDF.coalesce(1).write.format("csv").mode("overwrite").save(data_path_small + "users.csv")

val commitsSmallDF = commitsDF.limit(10000)
commitsSmallDF.coalesce(1).write.format("csv").mode("overwrite").save(data_path_small + "commits.csv")

val pullRequestsSmallDF = pullRequestsDF.limit(10000)
pullRequestsSmallDF.coalesce(1).write.format("csv").mode("overwrite").save(data_path_small + "pull_requests.csv")

val projectsSmallDF = projectsDF.limit(5000)
projectsSmallDF.coalesce(1).write.format("csv").mode("overwrite").save(data_path_small + "projects.csv")

val projectLanguagesSmallDF = projectLanguagesDF.limit(3000)
projectLanguagesSmallDF.coalesce(1).write.format("csv").mode("overwrite").save(data_path_small + "project_languages.csv")
