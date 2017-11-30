package GraphAnalysis

import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.{Queue,HashMap,Set,HashSet,Buffer,SortedSet}
import scala.collection.immutable
import org.apache.spark.broadcast.Broadcast
import Test_commons._
import java.io.{File, PrintWriter}

object Betweenness {

  /*
  * userSetForMovies - Hashmap with key as movie ids. The values are the set of users who rated the movie
  * usersIndex - Hashmap with key as original userIds and values are new indices which are continuous.
   */
  def main(args:Array[String]): Unit ={
    // val startTime = System.currentTimeMillis()
    val ratingsFilePath = args(0)
    val communitiesOutputPath = args(1)
    val betweennessOutputPath = args(2)
    val sc = makeSparkContext()
    val (userSetForMovies,usersIndex) = extractGraphData(sc,ratingsFilePath)
    val countOfRatings:Array[Int] = makeUpperTriangularMatrix(usersIndex,userSetForMovies)
    val nodes = userSetForMovies.values.flatten.toSet
    val indexUsers = usersIndex.map(_.swap)
    val numUsers = indexUsers.keySet.max
    // // println(numUsers)
    var edges = HashMap[Int,HashSet[Int]]()
    for(i <- 1 until numUsers){
        for(j<- i+1 to numUsers){
            val k = ((i-1)*(numUsers-i.toFloat/2)+(j-i)).toInt
            if(countOfRatings(k)>=1) {
                edges = addToSet(indexUsers(i),indexUsers(j),edges)
            }
        }
    }
    val edgesBV = sc.broadcast(edges)
    val betweennessScores = sc.parallelize(usersIndex.keySet.toSeq).mapPartitions(roots => calculateBetweennessMR(roots,edgesBV)).reduceByKey((v1,v2)=>(v1+v2)).collectAsMap.mapValues(x=>x/2)
    edgesBV.destroy()
    sc.stop()
    handleOutput(betweennessOutputPath,betweennessScores,edges)
    // println(s"The total execution time taken is ${(System.currentTimeMillis() - startTime)/(1000)} sec.")
  }

//bfsData is a combined variable for bfsMaps as well as parentsMaps. the positive keys are for bfsMaps and negative keys are for parentsMaps
  def calculateBetweennessMR(roots:Iterator[Int],edges:Broadcast[HashMap[Int,HashSet[Int]]])= {
    val betweennessScores = HashMap[immutable.Set[Int],Float]().withDefaultValue(0f)
    while(roots.hasNext){
      val root = roots.next
      val (bfsMap,parentsMap) = runBFS(root,edges.value)
      val bs = betweennessScore(bfsMap,parentsMap)
      bs.foreach(x => (betweennessScores(x._1)+=x._2))
    }
    betweennessScores.toIterator
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
    var userSetsForMovies = HashMap[Int,Set[Int]]()
    if (ind == 0) data.next()
    while (data.hasNext){
      val line = data.next()
      val lineSplit = line.split(",")
      val movieId=lineSplit(1).toInt
      val userId=lineSplit(0).toInt
      if (userSetsForMovies.contains(movieId)){
        userSetsForMovies(movieId) += userId
      } else {
        userSetsForMovies(movieId) = Set(userId)
      }
    }
    userSetsForMovies.iterator
  }

  /*
  * makes a hashmap in which keys are the original userIds and values are new indices.
  * The purpose is to make the user indices continuous.
  * This is required to build a upper triangular matrix.
  * */
  def makeUsersIndex(setOfUsers: immutable.Set[Int]) = {
    val usersIndex = new HashMap[Int,Int]()
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
  def makeUpperTriangularMatrix(usersIndex:HashMap[Int,Int], userSetsForMovies:scala.collection.Map[Int,Set[Int]])={
    val numUsers = usersIndex.size
    var temp = usersIndex.map(_.swap)
    val edgeCountMatrix = new Array[Int](numUsers*numUsers/2)
    for((_,userSet)<-userSetsForMovies){
      for(pair <- userSet.subsets(2)){
        val i=pair.map(x=>usersIndex(x)).min
        val j=pair.map(x=>usersIndex(x)).max
        val k = ((i-1)*(numUsers-i.toFloat/2)+(j-i)).toInt
       // println(s"Incrementing for Actual indices: ${temp(i)},${temp(j)} =========> ${k} ")
       // println(s"Incrementing for calculated indices: ${i},${j} =========> ${k} ")
        edgeCountMatrix(k) += 1
      }
    }
    edgeCountMatrix
  }

  // def createEdgeFrame(sc:SparkContext, numUsers:Int, edgeCounts:Array[Int], minCommonElements:Int): DataFrame ={
  //   var edgeList = Set[Row]()
  //   val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  //   for(i <- 1 until numUsers){
  //     for(j <- i+1 to numUsers){
  //       val k = ((i-1)*(numUsers-i.toFloat/2)+(j-i)).toInt
  //       if(edgeCounts(k)>=minCommonElements) {

  //         //          val smallerNode = math.min(indexUsers(i),indexUsers(j))
  //         //          val largerNode = math.max(indexUsers(i),indexUsers(j))
  //         //          edgeList += (Row(smallerNode,largerNode))
  //         //          println(s"there is an edge etween ${indexUsers(i)} and ${indexUsers(j)}")
  //         edgeList += (Row(i,j))
  //         edgeList += (Row(j,i))
  //       }
  //     }
  //   }
  //   val structEdge = new StructType().add("src", IntegerType).add("dst",IntegerType)
  //   val edgeRDD = sc.parallelize(edgeList.toSeq)
  //   val edgeFrame = sqlContext.createDataFrame(edgeRDD,structEdge)
  //   edgeFrame
  // }

  def handleOutput(fileName:String,betweennessScores:scala.collection.Map[immutable.Set[Int],Float], edges:HashMap[Int,HashSet[Int]]) = {
    val file = new File(fileName)
    val pw = new PrintWriter(file)
    val sortedEdges = edges.keySet.toSeq.sorted
    for(edge <- sortedEdges){
      val adjList = edges(edge).filter(_>edge).toSeq.sorted
      adjList.foreach(x=>pw.write(s"(${edge},${x},${betweennessScores(immutable.Set(edge,x))})\n"))
    }
    pw.close()
  }
  def handleOutput2(fileName:String,modularities:Buffer[Double]) = {
    val file = new File(fileName)
    val pw = new PrintWriter(file)
    modularities.foreach(x=>pw.write(s"$x\n"))
    pw.close()
  }

  def handleOutput3(fileName:String,communities:SortedSet[SortedSet[Int]]) = {
    val file = new File(fileName)
    val pw = new PrintWriter(file)
    communities.foreach(x=>pw.write(s"[${x.mkString(",")}]\n"))
    pw.close()
  }
}
