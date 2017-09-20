package edu.usc.sauravks

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel

object saurav_sahu_task2 {
  def main(args: Array[String]): Unit = {

    val appName = "Movie_Rating_Cal"
    val master = "local[*]" // uses as many cores as present in local machine
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    val sc = new SparkContext(conf) //spark context is the interface with cluster

    //Read the files into an RDD
    val users_file_path = args(0)
    val ratings_file_path =args(1)
    val movies_file_path = args(2)
    val storageLevel = StorageLevel.MEMORY_ONLY
    val dist_users_data = sc.textFile(users_file_path).persist(storageLevel)
    val dist_ratings_data = sc.textFile(ratings_file_path).persist(storageLevel)
    val dist_movies_data = sc.textFile(movies_file_path).persist(storageLevel)

    //key value pairs from users data
    val UID_gender_KV = dist_users_data.map(extract_UID_gender.generate_KV_pairs)
    val UID_movie_rating_KV = dist_ratings_data.map(extract_UID_movie_rating.generate_KV_pairs)
    val combined_KV_pair = UID_gender_KV.join(UID_movie_rating_KV)
    val movie_gender_rating_KV = combined_KV_pair.map(kv => kv._2)
      .map(kv => (kv._2._1,(kv._1,kv._2._2)))

    //generate movie genre data
    val movie_genre_KV = dist_movies_data.map(extract_movie_genre.generate_KV_pairs)
    val genre_gender_rating = movie_gender_rating_KV.join(movie_genre_KV)
      .map(kv => ((kv._2._2,kv._2._1._1),kv._2._1._2))
      .groupByKey()
      .mapValues(z => z.foldLeft(0,0){
        (acc,elmt) => (acc._1+elmt, acc._2+1)
      })
      .mapValues(z => z._1.toDouble / z._2.toDouble)
      .sortByKey()
      .map(kv => (kv._1._1,kv._1._2,kv._2))
      .collect()

    var output = ""
    genre_gender_rating.foreach({
      tup =>
        output += tup._1.toString + "," + tup._2 + "," + tup._3.toString + "\n"
    } )
    //clean up the memory
    output = output.stripLineEnd
    fileWriter.writeToFile(output, "saurav_sahu_result_task2.txt")
    dist_ratings_data.unpersist()
    dist_users_data.unpersist()
    dist_movies_data.unpersist()
    sc.stop()
  }
}

object extract_movie_genre {
  def generate_KV_pairs(line:String): (Int,String)= {
    val split_line =  line.split("::")
    (split_line(0).toInt,split_line(2))
  }
}