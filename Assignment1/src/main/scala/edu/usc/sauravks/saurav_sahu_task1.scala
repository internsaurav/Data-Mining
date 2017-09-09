package edu.usc.sauravks

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object saurav_sahu_task1 {
  def main(args: Array[String]): Unit = {
    println("Starting assignment")
    val appName = "Movie_Rating_Cal"
    val master = "local[*]" // uses as many cores as present in local machine
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    val sc = new SparkContext(conf) //spark context is the interface with cluster

    //Read the files
      val movies_file_path = "/home/saurav/Documents/Data Mining/Assignments/CSCI-541/Assignment1/ml-1m/movies.dat"
      val distributed_movies_file = sc.textFile(movies_file_path)
      distributed_movies_file.collect().foreach(println)
      sc.stop()
  }
}
