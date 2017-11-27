package GraphAnalysis
import org.scalatest.FunSuite
import GirvanNewman._
import org.apache.spark.SparkContext
import scala.collection.mutable
import Test_commons._

class AdlistImplementation extends FunSuite{

  test("AdjacencyList graph"){
    val ratingsFilePath = "../testInput/ratings4.csv"
    val sc = makeSparkContext()
    val (userSetForMovies,usersIndex) = extractGraphData(sc,ratingsFilePath)
    val nodes = userSetForMovies.values.flatten.toSet
    println(nodes.mkString("\n"))
    println(usersIndex.mkString("\n"))
    println(userSetForMovies.mkString("\n"))
    val countOfRatings:Array[Int] = makeUpperTriangularMatrix(usersIndex,userSetForMovies)
    val indexUsers = usersIndex.map(_.swap)
    val numUsers = indexUsers.keySet.max
    println(numUsers)
    val edges = mutable.HashMap[Int,mutable.Set[Int]]()
    for(i <- 1 until numUsers){
        for(j<- i+1 to numUsers){
            val k = ((i-1)*(numUsers-i.toFloat/2)+(j-i)).toInt
            if(countOfRatings(k)>=1) {
                addToSet(indexUsers(i),indexUsers(j),edges)
            }
        }
    }
    println(edges.mkString("\n"))
    sc.stop()
  }


}
