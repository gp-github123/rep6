package www.huayan.com.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

object _01ClassDemo {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("wordCount"))
    sc.setLogLevel("warn")
    val rdd1: RDD[String] = sc.textFile("hdfs://node01:8020/data/wordcount.txt")
    val flatMap: RDD[String] = rdd1.flatMap(_.split("\\s+"))
    val flatMap2: RDD[String] = rdd1.flatMap(_.split(" ")).filter(_.length>=6)
    val mapRdd: RDD[(String, Int)] = flatMap2.map((_, 1))
    val result = mapRdd.reduceByKey(_ + _).sortBy(_._2)
    result.collect().foreach(println(_))
  }
}
