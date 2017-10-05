package freuentItemsets

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import sun.security.util.BitArray

import scala.collection.mutable.BitSet

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
    val male = "M"
    val female = "F"
    val userGenderBitVector:BitArray = findUsers(sc,usersFile,male,numUsers)


    sc.stop()

//    APriori.runApriori()
  }

  def findUsers(sc: SparkContext ,filename: String, gender: String,numUsers:Int):BitArray = {
    val path = new File(filename).getCanonicalPath
    val storageLevel = StorageLevel.MEMORY_ONLY
    val dist_users_data = sc.textFile(path).persist(storageLevel) //creates RDDs of Strings
    val UID_gender_KV = dist_users_data.map(line => extract_UID_gender.generate_KV_pairs(line,gender)).collect()
    dist_users_data.unpersist()
    var arr = new Array[Int](numUsers+1)
    var userGenderBitVector = new BitArray(numUsers+1)
    for (index <- UID_gender_KV){
      userGenderBitVector.set(index,true)
    }
    userGenderBitVector
  }

  object extract_UID_gender {
    def generate_KV_pairs(line:String, gender: String): Int = {
      val split_line =  line.split("::")
      if(split_line(1) == gender) split_line(0).toInt else 0
    }
  }
}