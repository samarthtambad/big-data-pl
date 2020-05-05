# Config
```
scala - 2.11.8
spark - 2.3.0.cloudera4
# Java HotSpot(TM) 64-Bit Server VM, Java 1.8.0_162
```

# Steps
Starting at the root of the project, following the following steps to run the application.

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
Once finished, the compiled classes will be saved to ```target/scala-2.11/``` directory.

### Package into a jar file
```
sbt package
```
Once finished, the compiled jar file will be saved to ```target/scala-2.11/``` directory.

### Submitting jobs to cluster
```
spark2-submit --name "<your job name>" --class <your main class> --master yarn --deploy-mode cluster --verbose --driver-memory 5G --executor-memory 2G --num-executors 10 target/scala-2.11/<your JAR>.jar
```

### Running spark-shell in with configurable memory and executors
```
spark2-shell --name "<your job name>"" --master yarn --deploy-mode client --verbose --driver-memory 5G --executor-memory 2G --num-executors 20
```

# Examples

### 1. Running test job
```
spark2-submit --name "TEST" --class test.Test --master yarn --deploy-mode cluster --verbose --driver-memory 5G --executor-memory 2G --num-executors 10 target/scala-2.11/big-data-pl_2.11-1.0.jar
```

### 2. Run jobs for Github data
#### ETL
```
spark2-submit --name "GH_ETL" --class etl.TransformGithubRaw --master yarn --deploy-mode cluster --verbose --driver-memory 5G --executor-memory 2G --num-executors 20 target/scala-2.11/big-data-pl_2.11-1.0.jar
```

#### Profiling
```
spark2-submit --name "GH_PROFILE" --class profile.ProfileGithub --master yarn --deploy-mode cluster --verbose --driver-memory 5G --executor-memory 2G --num-executors 20 target/scala-2.11/big-data-pl_2.11-1.0.jar
```

#### Analysis
```
spark2-submit --name "GH_ANALYZE" --class analysis.AnalyzeGithub --master yarn --deploy-mode cluster --verbose --driver-memory 5G --executor-memory 2G --num-executors 20 target/scala-2.11/big-data-pl_2.11-1.0.jar
```

### 3. Run jobs for StackOverflow data
#### ETL
```
spark2-submit --packages com.databricks:spark-xml_2.11:0.9.0 --name "SO_ETL" --class etl.TransformStackOverflowRaw --master yarn --deploy-mode cluster --verbose --driver-memory 5G --executor-memory 2G --num-executors 20 target/scala-2.11/big-data-pl_2.11-1.0.jar
```

#### Profiling
```
spark2-submit --packages com.databricks:spark-xml_2.11:0.9.0 --name "SO_PROFILE" --class etl.ProfileStackOverflow --master yarn --deploy-mode cluster --verbose --driver-memory 5G --executor-memory 2G --num-executors 20 target/scala-2.11/big-data-pl_2.11-1.0.jar
```

#### Analysis
```
spark2-submit --packages com.databricks:spark-xml_2.11:0.9.0 --name "SO_ANALYZE" --class etl.AnalyzeStackOverflow --master yarn --deploy-mode cluster --verbose --driver-memory 5G --executor-memory 2G --num-executors 20 target/scala-2.11/big-data-pl_2.11-1.0.jar
```
