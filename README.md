
<h1> Big Data Application Project
<a href="https://arxiv.org/pdf/2006.01351.pdf">
    <img align="right" src="https://img.shields.io/badge/arXiv-2006.01351-blue" class="build-status" alt="build status">
</a>
<span style="float:right; padding-right:10px; width: 5px">&nbsp;</span>
<a href="https://public.tableau.com/profile/samarth.tambad#!/vizhome/ProgrammingLanguagesAnalysis/Dashboard1">
    <img align="right" src="https://img.shields.io/badge/visualization-Tableu-blue" class="build-status" alt="build status">
</a>
</h1>

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