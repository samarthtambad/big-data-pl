
// set path to data
val data_path: String = "project/data/raw/data/"

// load project_languages.csv file
val project_languages = sc.textFile(data_path + "project_languages.csv")

// save fields 1 & 2 as tuple (project_id, language)
val project_languages = sc.textFile(data_path + "project_languages.csv").map(line => line.split(",")).map(arr => (arr(0), arr(1)))

// save fields 3 & 4 as tuple (project_id, committer_id)
val commits = sc.textFile(data_path + "commits.csv").map(line => line.split(",")).map(arr => (arr(4), arr(3)))

// find number of commits for each project - (project_id, #commits)
val commits_projects = commits.map(tup => tup._1)

// join the two to get (project_id, (language, committer_id))
val commits_language = project_languages.join(commits)


