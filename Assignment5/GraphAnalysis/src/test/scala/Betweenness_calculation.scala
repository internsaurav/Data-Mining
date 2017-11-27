package GraphAnalysis
import org.scalatest.FunSuite
import GirvanNewman._
import org.apache.spark.SparkContext
import scala.collection.mutable.{Queue,HashMap,Set}
import scala.collection.immutable
import test_commons._

class Betweenness_calculation extends FunSuite{

  test("Betweenness_calculation"){
    val ratingsFilePath = "../ratings.csv"
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
    // val bfsMapE = bfsMaps(5)
    // val parentsMapE = parentsMaps(5)
    // println(s"BFS map for E: ${bfsMapE}")
    // println(s"parents map for E: ${parentsMapE}")
    sc.stop()
  }

  // def betweennessScore(bfsMap : HashMap[Int,immutable.Set[Int]],parentsMaps:HashMap[Int,Set[Int]])={

  // }
}
