# Steps for processing data

## Data Collection
Collect the data from [source](https://ghtorrent.org/). I contains a mysql dump in csv format with one file per table.
```
mysql-dump-github
├── commit_comments.csv
├── commit_parents.csv
├── commits.csv
├── followers.csv
├── ght-restore-mysql
├── indexes.sql
├── issue_comments.csv
├── issue_events.csv
├── issue_labels.csv
├── issues.csv
├── ORDER
├── organization_members.csv
├── project_commits.csv
├── project_languages.csv
├── project_members.csv
├── projects.csv
├── project_topics.csv
├── pull_request_comments.csv
├── pull_request_commits.csv
├── pull_request_history.csv
├── pull_requests.csv
├── README.md
├── repo_labels.csv
├── repo_milestones.csv
├── schema.sql
├── users.csv
└── watchers.csv
```

It is available as a tar.gz file. Extract it using ```tar -xzf filename.tar.gz```. Move it to hdfs to be processed using Spark.

## Data Preparation
Perform Extract, Transform and Load (ETL) step where data from multiple sources are extracted into the staging area (HDFS in this case). Then transformed to a suitable format by removing unwanted/duplicate rows, correcting data format, combining data from multiple tables into one table, etc. Finally load the data back into the staging area for further processing.

## Data Processing


## Data Storage

