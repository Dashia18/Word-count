package com.epam.sample.spark

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("WordCount")
      //.setMaster("local[*]")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(args(0))
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_+_)
    counts.saveAsTextFile(args(1))

    Thread.sleep(10*60*1000) //10 minutes

    println("Hello, Spark work!")
    sc.stop()
  }
}
