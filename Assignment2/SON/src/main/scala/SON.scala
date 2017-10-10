package frequentItemsets

import java.io.File

import frequentItemsets.APriori.updateCandidatePair
import org.apache.spark.{Accumulator, SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import scodec.bits.BitVector

import scala.collection.immutable.{HashSet, SortedSet}
import scala.collection.mutable
import scala.util.control.Breaks.{break, breakable}

object SON {

  def main(args: Array[String])={

    //define spark context
    val appName = "SON"
    val master = "local[*]" // uses as many cores as present in local machine
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    val sc = new SparkContext(conf) //spark context is the interface with cluster

    //input parameters
    val caseNumber = 2
    val usersFile = "/home/saurav/Documents/Data Mining/Assignments/CSCI-541/Data/ml-1m/test_users.dat"
    val ratingsFile = "/home/saurav/Documents/Data Mining/Assignments/CSCI-541/Data/ml-1m/test_ratings.dat"
    val support = sc.broadcast(4)
    var gender = sc.broadcast("")
    if (caseNumber == 1) gender = sc.broadcast("M") else gender = sc.broadcast("F")
    val userGenderBitVector = sc.broadcast(findUsers(sc,usersFile,gender))
    val baskets = makeBaskets(sc, ratingsFile, userGenderBitVector,caseNumber)
    gender.destroy()
    userGenderBitVector.destroy()
    val numBaskets = sc.broadcast(baskets.length)
    var basketsRDD = sc.parallelize(baskets)
//    val x = APriori.runApriori(basketsRDD.toLocalIterator,1300.toFloat/baskets.length).map(emitSortedCI)
//    for(kv<-x){
//      val c = kv._2
//      for (z <- c){
//        println(z)
//      }
//    }

    val candidateItemSets = basketsRDD.mapPartitions(i => callAprioriOnPartition(i,support,numBaskets)).reduceByKey(joinSets).collectAsMap()
//    println(candidateItemSets.mkString("\n"))

    runPhase2SON(sc,baskets,candidateItemSets,support)
    numBaskets.destroy()
    support.destroy()
    sc.stop()
  }

  def joinSets(x:HashSet[Set[Int]],y:HashSet[Set[Int]]):HashSet[Set[Int]]={
    x.union(y)
  }

  def callAprioriOnPartition(iterator: Iterator[Iterable[Int]], support: Broadcast[Int],numBaskets: Broadcast[Int]):Iterator[(Int,HashSet[Set[Int]])]={
    val supportPerBasket = support.value.toFloat/numBaskets.value
    APriori.runApriori(iterator, supportPerBasket)
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

  def runPhase2SON(sc: SparkContext, baskets: Array[Iterable[Int]], candidateItemSets: scala.collection.Map[Int, HashSet[Set[Int]]], support: Broadcast[Int]): Unit = {
    var frequentItemsSets = new mutable.HashMap[Int,HashSet[Set[Int]]]()
    val candidateItems = sc.broadcast(candidateItemSets)
    val basketsRDD = sc.parallelize(baskets)
//    val fre = basketsRDD.mapPartitions(data => countOccurenceOfCandidateItemsInPartition(data,candidateItems)).reduceByKey((x,y) => joinMaps(x,y,supportBV)).map(x=>emitSortedFrequentItems(x)).collectAsMap()
      val fre = basketsRDD.mapPartitions(data => countOccurenceOfCandidateItemsInPartition(data,candidateItems)).reduceByKey((x,y) => joinMaps(x,y)).mapValues(x=>x.retain((k,v) => v>=support.value)).map(x=>emitSortedFrequentItems(x)).collectAsMap()
    println(fre.mkString("\n"))
    for((k,v) <- fre){
      v.foreach(println)
    }
  }

  def emitSortedFrequentItems(x: (Int, mutable.HashMap[Set[Int], Int])): (Int,Set[Set[Int]])= {
    val v = x._2.keySet
    val ordering = Ordering.by[Set[Int],Iterable[Int]](_.toIterable)
    val s = SortedSet[Set[Int]]()(ordering)++v
    (x._1,s)
  }

  private def emitSortedCI(x: (Int, HashSet[Set[Int]])): (Int,Set[Set[Int]])= {
    val v = x._2
    val ordering = Ordering.by[Set[Int],Int](_.min)
    val s = SortedSet[Set[Int]]()(ordering)++v
    (x._1,s)
  }


  def joinMaps(x:mutable.HashMap[Set[Int],Int], y:mutable.HashMap[Set[Int],Int]): mutable.HashMap[Set[Int],Int]={
    for ((k,v) <- y){
      if (x.contains(k)) x(k) += v else x(k) = v
    }
    x
  }


  def countOccurenceOfCandidateItemsInPartition(dataRDD: Iterator[Iterable[Int]], candidateItems: Broadcast[collection.Map[Int, HashSet[Set[Int]]]]): Iterator[(Int,mutable.HashMap[Set[Int],Int])] = {
    val data = dataRDD.toIterable
    var countsMap = new mutable.HashMap[Int,mutable.HashMap[Set[Int],Int]]()
    for (basket <- data){
      val basketSet = basket.toSet
      for ((size,itemSets) <- candidateItems.value){
        for (itemSet <- itemSets){
          if (itemSet.subsetOf(basketSet)){
            if (countsMap.contains(size)){
              var comboCountMap = countsMap(size)
              if (comboCountMap.contains(itemSet)) countsMap(size)(itemSet) += 1 else countsMap(size)(itemSet) = 1
            } else {
              countsMap(size) = new mutable.HashMap[Set[Int],Int](){itemSet -> 1}
            }
          }
        }
      }
    }
    countsMap.iterator
  }



  //phase 2 SON. here is where we count
//  def runPhase2SON(sc: SparkContext, baskets: Array[Iterable[Int]], candidateItemSets: scala.collection.Map[Int, HashSet[Set[Int]]], support: Int): Unit = {
//    var frequentItemsSets = new mutable.HashMap[Int,HashSet[Set[Int]]]()
//    val maxComboSize = sc.broadcast(candidateItemSets.keySet.max)
//    val totalCandidates = candidateItemSets.values.flatten.size
//    //accumArray is an array of accumulators for each candidate set
//    val accumArray= new Array[Accumulator[Int]](totalCandidates)
//    //counts map maps each set to an index in the accum array
//    var countsMap = new mutable.HashMap[Int,mutable.HashMap[Set[Int],Int]]()
//    var accumIndex = 0
//    //add reference to accumulator index
//    for((size,candSet) <- candidateItemSets){
//      for (cand <- candSet){
//        accumArray(accumIndex)=sc.accumulator(0)
//        if (countsMap.contains(size)) countsMap(size) += (cand -> accumIndex) else countsMap(size) = new mutable.HashMap[Set[Int],Int](){cand -> accumIndex}
//        accumIndex +=1
//      }
//    }
//    val countsMapBV = sc.broadcast(countsMap)
////    val accumArrayBV = sc.broadcast(accumArray)
//
//    var basketsRDD = sc.parallelize(baskets)
//    basketsRDD.mapPartitions(data => countCombos(data,accumArray,countsMapBV,maxComboSize)).collect()
//    println(accumArray.mkString(","))
//    countsMapBV.destroy()
////    accumArrayBV.destroy()
//    maxComboSize.destroy()
//  }

//  def countCombos(dataRDD: Iterator[Iterable[Int]], accumArrayBV: Array[Accumulator[Int]], countsMapBV: Broadcast[mutable.HashMap[Int, mutable.HashMap[Set[Int], Int]]],maxComboSize: Broadcast[Int])= {
//    val data = dataRDD.toIterable
//    println("x")
//    for (basket <- data){
//      for (comboSize <- 1 to math.min(maxComboSize.value,basket.size)) {
//        val itr = basket.toSet.subsets(comboSize)
//        while (itr.hasNext) {
//          val combo = itr.next()
//          val accumMap = countsMapBV.value(comboSize)
//          if (accumMap.contains(combo)){
//            val accumIndx = accumMap(combo)
//            accumArrayBV(accumIndx) += 1
//          }
//        }
//      }
//    }
//    mutable.Iterable[Int]().toIterator
//  }
}
object SetOrdering extends Ordered[Set[Int]]{
  override def compare(that: Set[Int]): Int = that.min
}