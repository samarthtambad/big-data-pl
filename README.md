
<h1> Big Data Application Project
<a href="https://arxiv.org/pdf/2006.01351.pdf">
    <img align="right" src="https://img.shields.io/badge/arXiv-2006.01351-blue" class="build-status" alt="build status">
</a>
<span style="float:right; padding-right:10px;">&nbsp;</span>
<a href="https://public.tableau.com/profile/samarth.tambad#!/vizhome/ProgrammingLanguagesAnalysis/Dashboard1">
    <img align="right" src="https://img.shields.io/badge/visualization-Tableu-blue" class="build-status" alt="build status">
</a>
</h1>

Analysis of the community friendliness of a programming language from Github and StackOverflow data

## Data Sources
The following data was downloaded into HDFS as part of ingestion process:

#### 1. Github
Size: 102 GB \
Source: https://ghtorrent.org/
##### Data Locations on HDFS
Starting from root directory of user svt258 \
```Raw```: project/data/raw/data \
```Cleaned```: project/data/cleaned \
```Profiling```: project/data/stats \
```Analytics```: project/data/analysis

#### 2. StackOverflow
Size: 120 GB \
Source: https://archive.org/details/stackexchange
##### Data Locations on HDFS
Starting from root directory of user rhn235 \
```Raw```: project/data/raw/data \
```Cleaned```: project/data/cleaned \
```Profiling```: project/data/stats \
```Analytics```: project/data/analysis

## File Structure
```
.
├── build.sbt
├── data
│   ├── github_final_metrics.csv
│   └── stackoverflow_final_metrics.csv
├── project
│   ├── build.properties
│   └── target
│       ├── config-classes
│       ├── scala-2.12
│       └── streams
├── README.md
├── screenshots
├── src
│   └── main
│       └── scala
│           ├── app_code
│           │   ├── analysis_github.scala
│           │   └── analysis_stackoverflow.scala
│           ├── data_ingest
│           │   └── ingest.txt
│           ├── etl_code
│           │   ├── etl_github.scala
│           │   └── etl_stackoverflow.scala
│           ├── profiling_code
│           │   ├── profile_github.scala
│           │   └── profile_stackoverflow.scala
│           └── test_code
│               └── test.scala
└── target
    ├── scala-2.11
    │   ├── big-data-pl_2.11-1.0.jar
    │   ├── classes
    │   └── update
    └── streams
```
The ```data``` folder contains the final computed metrics each for GitHub and StackOverflow.
The compiled ```jar``` file can be found at location ```target/scala-2.11/```.

## Steps to run
Starting at the root of the project folder, perform the following steps to run the application.

### Load modules
```
module load spark/2.4.0
module load git/1.8.3.1
module load sbt/1.2.8
module load scala/2.11.8
```

### Compile project
``` 
sbt compile
```
Once finished, the compiled classes will be saved to ```target/scala-2.11/classes``` directory.

### Package into a jar file
```
sbt package
```
Once finished, the compiled jar file named ```big-data-pl_2.11-1.0.jar``` will be saved to ```target/scala-2.11/``` directory.

### Submitting jobs to cluster
```
spark2-submit --name "<your job name>" --class <your main class> --master yarn --deploy-mode cluster --verbose --driver-memory 5G --executor-memory 2G --num-executors 10 target/scala-2.11/<your JAR>.jar
```

### Running spark-shell in with configurable memory and executors
```
spark2-shell --name "<your job name>"" --master yarn --deploy-mode client --verbose --driver-memory 5G --executor-memory 2G --num-executors 20
```

# Examples

## 1. Running test job
```
spark2-submit --name "TEST" --class test.Test --master yarn --deploy-mode cluster --verbose --driver-memory 5G --executor-memory 2G --num-executors 10 target/scala-2.11/big-data-pl_2.11-1.0.jar
```
A random job just to test is spark-submit is working.

## 2. Run jobs for Github data
#### ETL
```
spark2-submit --name "GH_ETL" --class etl.TransformGithubRaw --master yarn --deploy-mode cluster --verbose --driver-memory 5G --executor-memory 2G --num-executors 20 target/scala-2.11/big-data-pl_2.11-1.0.jar
```
Reads data from the ```Raw``` data path in HDFS (as listed above) and stores cleaned data into the ```Cleaned``` data path.

#### Profiling
```
spark2-submit --name "GH_PROFILE" --class profile.ProfileGithub --master yarn --deploy-mode cluster --verbose --driver-memory 5G --executor-memory 2G --num-executors 20 target/scala-2.11/big-data-pl_2.11-1.0.jar
```
Reads data from the ```Cleaned``` data path in HDFS (as listed above) and stores tables generated with profiling info into the ```Profiling``` data path.

#### Analysis
```
spark2-submit --name "GH_ANALYZE" --class analysis.AnalyzeGithub --master yarn --deploy-mode cluster --verbose --driver-memory 5G --executor-memory 2G --num-executors 20 target/scala-2.11/big-data-pl_2.11-1.0.jar
```
Reads data from the ```Cleaned``` data path in HDFS (as listed above) and stores the computed metrics data into the ```Analytics``` data path.

## 3. Run jobs for StackOverflow data
#### ETL
```
spark2-submit --packages com.databricks:spark-xml_2.11:0.9.0 --name "SO_ETL" --class etl.TransformStackOverflowRaw --master yarn --deploy-mode cluster --verbose --driver-memory 5G --executor-memory 2G --num-executors 20 target/scala-2.11/big-data-pl_2.11-1.0.jar
```
Reads data from the ```Raw``` data path in HDFS (as listed above) and stores cleaned data into the ```Cleaned``` data path.

#### Profiling
```
spark2-submit --packages com.databricks:spark-xml_2.11:0.9.0 --name "SO_PROFILE" --class etl.ProfileStackOverflow --master yarn --deploy-mode cluster --verbose --driver-memory 5G --executor-memory 2G --num-executors 20 target/scala-2.11/big-data-pl_2.11-1.0.jar
```
Reads data from the ```Cleaned``` data path in HDFS (as listed above) and stores tables generated with profiling info into the ```Profiling``` data path.

#### Analysis
```
spark2-submit --packages com.databricks:spark-xml_2.11:0.9.0 --name "SO_ANALYZE" --class etl.AnalyzeStackOverflow --master yarn --deploy-mode cluster --verbose --driver-memory 5G --executor-memory 2G --num-executors 20 target/scala-2.11/big-data-pl_2.11-1.0.jar
```
Reads data from the ```Cleaned``` data path in HDFS (as listed above) and stores the computed metrics data into the ```Analytics``` data path.

# Configuration
```
scala - 2.11.8
spark - 2.3.0.cloudera4
# Java HotSpot(TM) 64-Bit Server VM, Java 1.8.0_162
Cluster UI: http://babar.es.its.nyu.edu:8088/cluster
```
