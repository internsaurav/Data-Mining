package frequentItemsets

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import scodec.bits.BitVector

import scala.collection.mutable

object SON {
  def main(args: Array[String])={

    //define spark context
    val appName = "SON"
    val master = "local[*]" // uses as many cores as present in local machine
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    val sc = new SparkContext(conf) //spark context is the interface with cluster

    //input parameters
    val caseNumber = 1
    val usersFile = "/home/saurav/Documents/Data Mining/Assignments/CSCI-541/Data/ml-1m/users.dat"
    val ratingsFile = "/home/saurav/Documents/Data Mining/Assignments/CSCI-541/Data/ml-1m/ratings.dat"
    val support = 1300
    val numUsers = 6040
    val male = sc.broadcast("M")
    val female = sc.broadcast("F")
    val userGenderBitVector = sc.broadcast(findUsers(sc,usersFile,male,numUsers))

    //now I want to generate the userId, movies baskets
    val userMovieBaskets = makeMovieBaskets(sc,ratingsFile,userGenderBitVector)
    male.destroy()
    userGenderBitVector.destroy()
    APriori.runApriori(userMovieBaskets,support)
    sc.stop()

  }
  def makeMovieBaskets(sc: SparkContext, filename:String, userGenderBitVector:Broadcast[BitVector]): Array[Iterable[Int]] = {
    val path = new File(filename).getCanonicalPath
    val storageLevel = StorageLevel.MEMORY_ONLY
    val dist_ratings_data = sc.textFile(path).persist(storageLevel) //creates RDDs of Strings
//    val movieBasketsKV = dist_ratings_data.map(line => generate_user_movie_KV_pairs(line,userGenderBitVector)).groupByKey().collect()
    val movieBasketsKV = dist_ratings_data.map(line => generate_user_movie_KV_pairs(line,userGenderBitVector)).groupByKey().map(user_movies => user_movies._2).collect()
    dist_ratings_data.unpersist()
    movieBasketsKV
  }

//  def makeMovieBaskets(sc: SparkContext, filename:String, userGenderBitVector:Broadcast[BitVector]): Array[(Int,Iterable[Int])] = {
//    val path = new File(filename).getCanonicalPath
//    val storageLevel = StorageLevel.MEMORY_ONLY
//    val dist_ratings_data = sc.textFile(path).persist(storageLevel) //creates RDDs of Strings
//    val movieBasketsKV = dist_ratings_data.map(line => generate_user_movie_KV_pairs(line,userGenderBitVector)).groupByKey().collect()
//    dist_ratings_data.unpersist()
//    movieBasketsKV
//  }

  def generate_user_movie_KV_pairs(line:String, userGenderBitVector: Broadcast[BitVector]): (Int,Int) = {
    val split_line =  line.split("::")
    val uid = split_line(0).toInt
    if(userGenderBitVector.value.get(uid)) (uid,split_line(1).toInt) else (0,0)
  }

  def findUsers(sc: SparkContext, filename: String, gender: Broadcast[String], numUsers:Int):BitVector = {
    val path = new File(filename).getCanonicalPath
    val storageLevel = StorageLevel.MEMORY_ONLY
    val dist_users_data = sc.textFile(path).persist(storageLevel) //creates RDDs of Strings
    val UID_gender_KV = dist_users_data.map(line => generate_gender_KV_pairs(line,gender)).collect()
    dist_users_data.unpersist()
    var arr = new Array[Int](numUsers+1)
    var userGenderBitVector = BitVector.low(numUsers+1)
    for (index <- UID_gender_KV){
      userGenderBitVector=userGenderBitVector.set(index)
    }
    userGenderBitVector
  }

  def generate_gender_KV_pairs(line:String, gender: Broadcast[String]): Int = {
    val split_line =  line.split("::")
    if(split_line(1) == gender.value) split_line(0).toInt else 0
  }
}