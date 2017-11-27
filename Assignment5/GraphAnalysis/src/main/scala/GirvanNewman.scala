package GraphAnalysis

import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import scala.collection.mutable.{Queue,HashMap,Set}
import scala.collection.immutable

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
    val nodes = userSetForMovies.values.flatten.toSet
    val indexUsers = usersIndex.map(_.swap)
    val numUsers = indexUsers.keySet.max
    // println(numUsers)
    val edges = HashMap[Int,Set[Int]]()
    for(i <- 1 until numUsers){
        for(j<- i+1 to numUsers){
            val k = ((i-1)*(numUsers-i.toFloat/2)+(j-i)).toInt
            if(countOfRatings(k)>=1) {
                addToSet(indexUsers(i),indexUsers(j),edges)
            }
        }
    }
    // println(edges.mkString("\n"))
    val bfsMaps = HashMap[Int,HashMap[Int,immutable.Set[Int]]]()
    val parentsMaps = HashMap[Int,HashMap[Int,Set[Int]]]()
    for (i <- usersIndex.keySet){
        // println(s"running BFS from Node $i")
        val (bfsMap,parentsMap) = runBFS(i,nodes,edges)
        bfsMaps += ((i,bfsMap))
        parentsMaps += ((i,parentsMap))
        // println("============================")
    }
    println("BFSMaps:")
    println(bfsMaps.mkString("\n"))
    println("parentsMaps:")
    println(parentsMaps.mkString("\n"))
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

  def createEdgeFrame(sc:SparkContext, numUsers:Int, edgeCounts:Array[Int], minCommonElements:Int): DataFrame ={
    var edgeList = Set[Row]()
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

  def addToSet(i:Int,j:Int,edges:HashMap[Int,Set[Int]]):Unit= {
    if (edges.contains(i)) edges(i) += j else edges += ((i,Set(j)))
    if (edges.contains(j)) edges(j) += i else edges += ((j,Set(i)))
  }

   /*
    *frontier - the main queue which holds the nodes to be visited next
    *bfsMap - the BFS tree, containing the nodes at level-wie distance from root
    *parentsMap- hashMap containing the parents of each node in this BFS tree
    *visitedNodes- set of nodes already visited in this traversal
    *distance - the level
    *thisLevelNodes -  nodes in a particular level
    *children- Children of the node. First finds the set of neighbours and removes the already visited nodes
    */
    def runBFS(root:Int,nodes:immutable.Set[Int],edges:HashMap[Int,Set[Int]]) = {
        val frontier = Queue[Int]()
        val bfsMap = HashMap[Int,immutable.Set[Int]]()
        val parentsMap = HashMap[Int,Set[Int]]()
        val visitedNodes = Set[Int]()
        var distance = 0
        frontier.enqueue(root)
        while(!frontier.isEmpty){
            // println(s"frontier ====> ${frontier.mkString(",")}")
            val thisLevelNodes = frontier.dequeueAll(x=>true)
            visitedNodes ++= thisLevelNodes
            bfsMap += ((distance,thisLevelNodes.toSet))
            val thisLevelNeighbours = Set[Set[Int]]()
            for(thisNode <- thisLevelNodes){
                val children = edges(thisNode)-- visitedNodes
                addParents(thisNode,children,parentsMap)
                thisLevelNeighbours += children
            }
            frontier ++= thisLevelNeighbours.flatten
            distance += 1
        }
        // println(bfsMap.mapValues(x=>x.map(y=>findNodeName(y))).mkString("\n"))
        // println(s"parents map ===> ${parentsMap.map(x =>(findNodeName(x._1),x._2.map(y=>findNodeName(y)))).mkString(",")}")
        (bfsMap,parentsMap)
    }

    def addParents(parent:Int,children:Set[Int],parentsMap:HashMap[Int,Set[Int]])={
        for(child<-children){
            if(parentsMap.contains(child)) parentsMap(child)+=parent else parentsMap += ((child,Set(parent)))
        }
    }
}
