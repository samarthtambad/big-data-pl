/* 
Github data
Source: https://ghtorrent.org/downloads.html
Size: 102 GB
Schema: https://ghtorrent.org/files/schema.pdf

The data is distributed among multiple csv files (one table per file). The relevant csv files are:
1. users.csv
2. commits.csv
3. pull_requests.csv
4. project_languages.csv

*/

// set path to data
val data_path: String = "project/data/raw/data/"

// load users.csv file into rdd
val usersRDD = sc.textFile(data_path + "users.csv").map(line => line.split(","))
val users_count_pre = usersRDD.count()

// load commits.csvfile into rdd
val commitsRDD = sc.textFile(data_path + "commits.csv").map(line => line.split(","))
val commits_count_pre = commitsRDD.count()

// load pull_requests.csv file into rdd
val pullRequestsRDD = sc.textFile(data_path + "pull_requests.csv").map(line => line.split(","))
val pull_requests_pre = pullRequestsRDD.count()

// load project_languages.csv file into rdd
val projectLanguagesRDD = sc.textFile(data_path + "project_languages.csv").map(line => line.split(","))
val project_languages_pre = projectLanguagesRDD.count()

