import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object CommunityDetection {

  /*
    * usersIndex - A hashmap with key as userId and value as index of user. Since the userIds are not continuous,
    * reindexing helps to save space while forming array with the users.
    * usersEdgesBoolArray - A 2D array with user indices on both axes, 0 means no edge and 1 means there is an edge.
    * */
  def main(args:Array[String]): Unit ={

    val ratingsFilePath = args(0)
    val communitiesOutputPath = args(1)
    val betweennessOutputPath = args(2)
    val sc = makeSparkContext()
//    val (usersIndex,usersEdgesBoolArray) = findNodesAndEdges(sc,ratingsFilePath)
    val userSetForMovies = findNodesAndEdges(sc,ratingsFilePath)
    sc.stop()
  }

  def makeSparkContext():SparkContext={
    val appName = "recSystem"
    val master = "local[*]" // uses as many cores as present in local machine
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    new SparkContext(conf) //spark context is the interface with cluster
  }

   /*
    * findNodesAndEdges parses the input to extract the nodes and the edges data.
    * userSetForMovies - Hashmap with key as movie ids. The values are the set of users who rated the movie
    * usersIndex - Hashmap with key as original userIds and values are new indices which are continuous.
    * */
  def findNodesAndEdges(sc: SparkContext, ratingsFilePath: String) = {
    val userSetForMovies = sc.textFile(ratingsFilePath)
      .mapPartitionsWithIndex((ind,itr)=>extractSetOfUsersForEachMovie(ind,itr))
      .reduceByKey((a,b)=>a.union(b))
      .collectAsMap()
    val setOfUsers = userSetForMovies.values.flatten.toSet
    val usersIndex = makeUsersIndex(setOfUsers)
    userSetForMovies
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

  def makeUpperTriangularMatri(usersIndex:mutable.HashMap[Int,Int],userSetsForMovies:scala.collection.Map[Int,mutable.Set[Int]])={
    val numUsers = usersIndex.size
    val edgeCountMatrix = new Array[Int](numUsers*numUsers/2-numUsers/2)
    for((_,userSet)<-userSetsForMovies){
      for(pair <- userSet.subsets(2)){
        val i=usersIndex(pair.min)
        val j=usersIndex(pair.max)
        val k = ((i-1)*(numUsers-i.toFloat/2)+(j-i)).toInt
        edgeCountMatrix(k) += 1
      }
    }
    edgeCountMatrix
  }
}
