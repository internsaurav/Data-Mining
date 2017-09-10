package edu.usc.sauravks

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel

object saurav_sahu_task1 {
  def main(args: Array[String]): Unit = {

    val appName = "Movie_Rating_Cal"
    val master = "local[*]" // uses as many cores as present in local machine
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    val sc = new SparkContext(conf) //spark context is the interface with cluster

    //Read the files into an RDD
    val users_file_path = "/home/saurav/Documents/Data Mining/Assignments/CSCI-541/Assignment1/ml-1m/users.dat"
    val ratings_file_path = "/home/saurav/Documents/Data Mining/Assignments/CSCI-541/Assignment1/ml-1m/ratings.dat"
    val output_file_path = "/home/saurav/Documents/Data Mining/Assignments/CSCI-541/Assignment1/ml-1m/task1.dat"
    val storageLevel = StorageLevel.MEMORY_ONLY
    val dist_users_data = sc.textFile(users_file_path).persist(storageLevel)
    val dist_ratings_data = sc.textFile(ratings_file_path).persist(storageLevel)

    //key value pairs from users data
    val UID_gender_KV = dist_users_data.map(extract_UID_gender.generate_KV_pairs)
    val UID_movie_rating_KV = dist_ratings_data.map(extract_UID_movie_rating.generate_KV_pairs)
    val combined_KV_pair = UID_gender_KV.join(UID_movie_rating_KV)
    val movie_gender_rating = combined_KV_pair.map(kv => kv._2)
      .map(kv => ((kv._2._1,kv._1),kv._2._2))
      .groupByKey()
      .mapValues(z => z.foldLeft(0,0){
        (acc,elmt) => (acc._1+elmt, acc._2+1)
      })
      .mapValues(z => z._1.toDouble / z._2.toDouble)
      .sortByKey()
      .map(kv => (kv._1._1,kv._1._2,kv._2))
      .collect()

    var output = ""
    movie_gender_rating.foreach({
      tup =>
        output += tup._1.toString + "," + tup._2 + "," + tup._3.toString + "\n"
    } )
    //clean up the memory
    output = output.stripLineEnd
    fileWriter.writeToFile(output)
    dist_ratings_data.unpersist()
    dist_users_data.unpersist()
    sc.stop()
  }
}

object extract_UID_gender {
  def generate_KV_pairs(line:String): (String,String)= {
    val split_line =  line.split("::")
    (split_line(0),split_line(1))
  }
}

object extract_UID_movie_rating {
  def generate_KV_pairs(line:String): (String,(Int,Int)) = {
    val split_line =  line.split("::")
    (split_line(0),(split_line(1).toInt,split_line(2).toInt))
  }
}

object fileWriter {
  def writeToFile (text : String): Unit = {
    val file = new File("output.dat")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(text)
    bw.close()
  }
}