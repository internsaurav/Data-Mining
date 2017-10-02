package freuentItemsets

import java.io.File
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel

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
    findUsers(sc,usersFile,"M",numUsers)

//    APriori.runApriori()
  }

  //find users makes an array of Bytes(1s and 0s). An integer takes 4 bytes. if 1/4th of users are of the gender we are looking for,
  //then its economical to make a Byte array
  // but I should use map reduce because the computation is expensive
  def findUsers(sc: SparkContext ,filename: String, gender: String,numUsers:Int) = {
    val path = new File(filename).getCanonicalPath
    val storageLevel = StorageLevel.MEMORY_ONLY
    val dist_users_data = sc.textFile(path).persist(storageLevel)
    val UID_gender_KV = dist_users_data.map(extract_UID_gender.generate_KV_pairs).



  }
  object extract_UID_gender {
    def generate_KV_pairs(line:String): (Int,String)= {
      val split_line =  line.split("::")
      // emit if gender is male
      if (split_line(1) == "M") {
        (split_line(0).toInt,split_line(1))
      }
    }
  }
}