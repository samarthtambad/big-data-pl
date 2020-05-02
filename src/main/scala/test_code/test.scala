package test

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Test {
    /*  
     *  For testing if/how spark submit works
     */

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder
        .appName("SubmitTest")
        .getOrCreate

        val sc = spark.sparkContext

        val file: String = "loudacre/weblog/2014-03-15.log"
        val accountFile: String = "loudacre/accounts/*"

        val dataRDD = sc.textFile(file)
        val accountDataRDD = sc.textFile(accountFile)

        val setupCountsRDD = dataRDD.map(line => (line.split(" ")(2), 1))
        val requestCountsRDD = setupCountsRDD.reduceByKey(_ + _)
        
        val visitFrequencyTotalsRDD = requestCountsRDD.map(tup => (tup._2, 1)).reduceByKey(_ + _).map(tup => (tup._2, tup._1))
        
        val userIdsRDD = accountDataRDD.map(line => line.split(",")(0))
        val userIdSet = userIdsRDD.collect().toSet
        val validAcctsIpsFinalRDD = dataRDD.filter(line => userIdSet(line.split(" ")(2))).map(line => (line.split(" ")(2), line.split(" ")(0))).groupByKey().map(tup => (tup._1, tup._2.toList))

        validAcctsIpsFinalRDD.saveAsTextFile("loudacre/weblog/test")
        
    }

}