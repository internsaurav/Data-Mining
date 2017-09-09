package edu.usc.sauravks

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object saurav_sahu_task1 {
  def main(args: Array[String]): Unit = {
    println("Starting assignment")
    val appName = "Movie_Rating_Calc"
    val master = "local[2]" // uses as many cores as present in local machine
    var conf = new SparkConf().setMaster(master).setAppName("Movie_Rating_Cal")
    val sc = new SparkContext(conf) //spark context is the interface with cluster
    sc.stop()

  }
}
