package GraphAnalysis
import org.scalatest.FunSuite
import GirvanNewman._
import org.apache.spark.SparkContext
import scala.collection.mutable.{Queue,HashMap,Set}
import scala.collection.immutable
import Test_commons._

class Betweenness_calculation extends FunSuite{

  test("Betweenness_calculation"){
    val ratingsFilePath = "../testInput/ratings4.csv"
    val sc = makeSparkContext()
    val (userSetForMovies,usersIndex) = extractGraphData(sc,ratingsFilePath)
    val nodes = userSetForMovies.values.flatten.toSet
    // println(nodes.mkString("\n"))
    // println(usersIndex.mkString("\n"))
    // println(userSetForMovies.mkString("\n"))
    val countOfRatings:Array[Int] = makeUpperTriangularMatrix(usersIndex,userSetForMovies)
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
    val edgesBV = sc.broadcast(edges)
    val bfsData = sc.parallelize(usersIndex.keySet.toSeq).mapPartitions(roots => runBFSinMR(roots,edgesBV)).collectAsMap()
    // nodesBV.destroy()
    edgesBV.destroy()
    sc.stop()
    val betweennessScores = new Array[Float](numUsers*numUsers/2)
    for (i<- 1 to numUsers){
        val bfsMapE = bfsData(i)
        val parentsMapE = bfsData(-i)
        // println(s"BFS map for E: ${bfsMapE}")
        // println(s"parents map for E: ${parentsMapE}")
        val bs = betweennessScore(bfsMapE,parentsMapE)
        // println(bs)
        for( (edgeSet,score) <- bs){
            val edge = edgeSet.toArray
            val i = math.min(usersIndex(edge(0)),usersIndex(edge(1)))
            val j = math.max(usersIndex(edge(0)),usersIndex(edge(1)))
            val k = ((i-1)*(numUsers-i.toFloat/2)+(j-i)).toInt
            betweennessScores(k)+=score
        }
    }
    for (i <- 1 until numUsers){
        for (j <- i+1 to numUsers){
            val k = ((i-1)*(numUsers-i.toFloat/2)+(j-i)).toInt
            println(s"betweenness of edge ${indexUsers(i)}-${indexUsers(j)} is ${betweennessScores(k)/2} ")
        }
    }
    
  }

  
}
