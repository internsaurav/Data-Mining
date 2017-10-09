package frequentItemsets

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import scodec.bits.BitVector

import scala.collection.immutable.HashSet
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
    var gender = sc.broadcast("")
    if (caseNumber == 1) gender = sc.broadcast("M") else gender = sc.broadcast("F")
    val userGenderBitVector = sc.broadcast(findUsers(sc,usersFile,gender))
    val baskets = makeBaskets(sc, ratingsFile, userGenderBitVector,caseNumber)
    gender.destroy()
    userGenderBitVector.destroy()
    var basketsRDD = sc.parallelize(baskets)
    val reducedSupport = sc.broadcast(1300/basketsRDD.getNumPartitions)
    val candidateItemSets = basketsRDD.mapPartitions(i => callAprioriOnPartition(i,reducedSupport)).reduceByKey(joinSets).collect()
    reducedSupport.destroy()
    sc.stop()
  }

  def joinSets(x:HashSet[Set[Int]],y:HashSet[Set[Int]]):HashSet[Set[Int]]={
    x.union(y)
  }

  def callAprioriOnPartition(iterator: Iterator[Iterable[Int]], support: Broadcast[Int]):Iterator[(Int,HashSet[Set[Int]])]={
    APriori.runApriori(iterator,support.value)
  }

  def makeBaskets(sc: SparkContext, filename: String, userGenderBitVector: Broadcast[BitVector], caseNumber: Int): Array[Iterable[Int]] = {
    val path = new File(filename).getCanonicalPath
    val storageLevel = StorageLevel.MEMORY_ONLY
    val dist_ratings_data = sc.textFile(path).persist(storageLevel) //creates RDDs of Strings
    val baskets = dist_ratings_data.map(line => generate_user_movie_KV_pairs(line, userGenderBitVector, caseNumber)).groupByKey().map(user_movies => user_movies._2).collect()
    dist_ratings_data.unpersist()
    baskets
  }

  /*
  * generates the key value pairs of mapping each basket name to 1 item of that basket
  * uid<userGenderBitVector.value.length - opposite gender users having UIDs greater than the max of Uid of this gender are also rejected
  * */
  def generate_user_movie_KV_pairs(line: String, userGenderBitVector: Broadcast[BitVector], caseNumber: Int): (Int,Int) = {
    val split_line =  line.split("::")
    val uid = split_line(0).toInt
    if( uid<userGenderBitVector.value.length &&  userGenderBitVector.value.get(uid)) {
      if (caseNumber == 1) (uid,split_line(1).toInt) else (split_line(1).toInt,uid)
    } else (0,0)
  }

  /*
  * findUsers returns the users of a specific gender
  * @sc - the sparkcontext
  * @filename- Name of the Users file
  * @gender- the specific gender we want to find
  * */
  def findUsers(sc: SparkContext, filename: String, gender: Broadcast[String]):BitVector = {
    val path = new File(filename).getCanonicalPath
    val storageLevel = StorageLevel.MEMORY_ONLY
    val dist_users_data = sc.textFile(path).persist(storageLevel) //creates RDDs of Strings
    val UID_gender_KV = dist_users_data.map(line => generate_gender_KV_pairs(line,gender)).collect()
    val numUsers = UID_gender_KV.max
    dist_users_data.unpersist()
    var userGenderBitVector = BitVector.low(numUsers+1)
    for (index <- UID_gender_KV){
      userGenderBitVector=userGenderBitVector.set(index)
    }
    userGenderBitVector
  }

    /*
    * generates uid users of specific gender
    * */
  def generate_gender_KV_pairs(line:String, gender: Broadcast[String]): Int = {
    val split_line =  line.split("::")
    if(split_line(1) == gender.value) split_line(0).toInt else 0
  }
}