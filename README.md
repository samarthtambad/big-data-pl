# Big Data Application Project
Analysis of the community friendliness of a programming language from Github and StackOverflow data

## Data Sources

### Github
Size: 102 GB \
https://ghtorrent.org/ \
More info [here](docs/README.md)

### StackOverflow
Size: 120 GB \
https://archive.org/details/stackexchange \
More info [here](docs/README.md)

## File Structure
```
.
├── build.sbt
├── data
│   ├── github_final_metrics.csv
│   └── stackoverflow_final_metrics.csv
├── src
│   └── main
│       └── scala
│           ├── app_code
│           │   ├── analysis_github.scala
│           │   └── analysis_stackoverflow.scala
│           ├── etl_code
│           │   ├── etl_github.scala
│           │   └── etl_stackoverflow.scala
│           ├── profiling_code
│           │   ├── profile_github.scala
│           │   └── profile_stackoverflow.scala
│           └── test_code
│               └── test.scala
├── steps.txt
├── README.md
└── visualization
    ├── final.twb
    └── github_final_metrics+.hyper
```
The ```data``` folder contains the final computed metrics each for GitHub and StackOverflow.

## Steps to run
1. [steps](usage.md)

http://babar.es.its.nyu.edu:8088/cluster

## References
1. https://en.wikipedia.org/wiki/Measuring_programming_language_popularity
2. https://github.blog/2015-08-19-language-trends-on-github/
3. 