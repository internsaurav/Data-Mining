package GraphAnalysis

import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.collection.mutable

object GirvanNewman {

  /*
  * userSetForMovies - Hashmap with key as movie ids. The values are the set of users who rated the movie
  * usersIndex - Hashmap with key as original userIds and values are new indices which are continuous.
   */
  def main(args:Array[String]): Unit ={
    val ratingsFilePath = args(0)
    val communitiesOutputPath = args(1)
    val betweennessOutputPath = args(2)
    val sc = makeSparkContext()
    val (userSetForMovies,usersIndex) = extractGraphData(sc,ratingsFilePath)
    val countOfRatings:Array[Int] = makeUpperTriangularMatrix(usersIndex,userSetForMovies)
    sc.stop()
  }

  def makeSparkContext():SparkContext={
    val appName = "GirvanNewman"
    val master = "local[*]" // uses as many cores as present in local machine
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    new SparkContext(conf) //spark context is the interface with cluster
  }

  /*
    * extractGraphData parses the input to extract the nodes and the edges data.
    * userSetForMovies - Hashmap with key as movie ids. The values are the set of users who rated the movie
    * setOfUsers - set of all users
    * usersIndex - Hashmap with key as original userIds and values are new indices which are continuous.
    * */
  def extractGraphData(sc: SparkContext, ratingsFilePath: String) = {
    val userSetForMovies = sc.textFile(ratingsFilePath)
      .mapPartitionsWithIndex((ind,itr)=>extractSetOfUsersForEachMovie(ind,itr))
      .reduceByKey((a,b)=>a.union(b))
      .collectAsMap()
    val setOfUsers = userSetForMovies.values.flatten.toSet
    val usersIndex = makeUsersIndex(setOfUsers)
    (userSetForMovies,usersIndex)
  }

  /*
 * generates the set of userIds for each movie. A userId is in the set for a movie if the userId has rated the movie
 *
 * */
  private def extractSetOfUsersForEachMovie(ind:Int, data:Iterator[String])={
    var userSetsForMovies = mutable.HashMap[Int,mutable.Set[Int]]()
    if (ind == 0) data.next()
    while (data.hasNext){
      val line = data.next()
      val lineSplit = line.split(",")
      val movieId=lineSplit(1).toInt
      val userId=lineSplit(0).toInt
      if (userSetsForMovies.contains(movieId)){
        userSetsForMovies(movieId) += userId
      } else {
        userSetsForMovies(movieId) = mutable.Set(userId)
      }
    }
    userSetsForMovies.iterator
  }

  /*
  * makes a hashmap in which keys are the original userIds and values are new indices.
  * The purpose is to make the user indices continuous.
  * This is required to build a upper triangular matrix.
  * */
  def makeUsersIndex(setOfUsers: Set[Int]) = {
    val usersIndex = new mutable.HashMap[Int,Int]()
    var i= 1
    for (user <- setOfUsers ){
      usersIndex += ((user,i))
      i +=1
    }
    usersIndex
  }

  /*
  * prepares the upper triangular matrix representing the number of movies that were rated in common between any two users
  * Actually
  * edgeCountMatrix = new Array[Int](numUsers*numUsers/2 - numUsers/2)
  * but since the indexing is started from 1 and not from 0 we have to use the formula that is used in  the this.
  * */
  def makeUpperTriangularMatrix(usersIndex:mutable.HashMap[Int,Int], userSetsForMovies:scala.collection.Map[Int,mutable.Set[Int]])={
    val numUsers = usersIndex.size
    var temp = usersIndex.map(_.swap)
    val edgeCountMatrix = new Array[Int](numUsers*numUsers/2)
    for((_,userSet)<-userSetsForMovies){
      for(pair <- userSet.subsets(2)){
        val i=pair.map(x=>usersIndex(x)).min
        val j=pair.map(x=>usersIndex(x)).max
        val k = ((i-1)*(numUsers-i.toFloat/2)+(j-i)).toInt
//        println(s"Incrementing for Actual indices: ${temp(i)},${temp(j)} =========> ${k} ")
//        println(s"Incrementing for calculated indices: ${i},${j} =========> ${k} ")
        edgeCountMatrix(k) += 1
      }
    }
    edgeCountMatrix
  }

  def createEdgeFrame(sc:SparkContext, numUsers:Int, edgeCounts:Array[Int], minCommonElements:Int): DataFrame ={
    var edgeList = mutable.Set[Row]()
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    for(i <- 1 until numUsers){
      for(j <- i+1 to numUsers){
        val k = ((i-1)*(numUsers-i.toFloat/2)+(j-i)).toInt
        if(edgeCounts(k)>=minCommonElements) {

          //          val smallerNode = math.min(indexUsers(i),indexUsers(j))
          //          val largerNode = math.max(indexUsers(i),indexUsers(j))
          //          edgeList += (Row(smallerNode,largerNode))
          //          println(s"there is an edge etween ${indexUsers(i)} and ${indexUsers(j)}")
          edgeList += (Row(i,j))
          edgeList += (Row(j,i))
        }
      }
    }
    val structEdge = new StructType().add("src", IntegerType).add("dst",IntegerType)
    val edgeRDD = sc.parallelize(edgeList.toSeq)
    val edgeFrame = sqlContext.createDataFrame(edgeRDD,structEdge)
    edgeFrame
  }
}
