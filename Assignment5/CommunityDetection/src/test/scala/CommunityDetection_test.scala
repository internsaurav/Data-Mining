import org.scalatest.FunSuite
import CommunityDetection._
import scala.collection.mutable

class CommunityDetection_test extends FunSuite{
  val ratingsFilePath = "../ratings.csv"


  test("userSetTests"){
    val sc = CommunityDetection.makeSparkContext()
    val temp = CommunityDetection.findNodesAndEdges(sc,ratingsFilePath)
    val allUsers = temp.values.flatten.toSet
//    println(allUsers.mkString("\n"))
    println(allUsers.size)
    println(temp.size)
    sc.stop()
  }

  test("makeUserIndex") {
    val sc = CommunityDetection.makeSparkContext()
    val temp = CommunityDetection.findNodesAndEdges(sc, ratingsFilePath)
    val allUsers = temp.values.flatten.toSet
    val temp2 = makeUsersIndex(allUsers)
    println(temp2.values.max)
    sc.stop()
  }

  test("triaMat"){
    val n = 650
    println(s"numUsers ===> ${n*n/2-n/2}")
    for(i <- 1 until n){
      for (j <- i+1 to n){
        println(s"${i},${j}===> ${((i-1)*(n-i.toFloat/2)+(j-i)).toInt}")
      }
    }
  }

  test("triangularMatrixTest"){
    val sc = CommunityDetection.makeSparkContext()
    val temp = CommunityDetection.findNodesAndEdges(sc, ratingsFilePath)
    val allUsers = temp.values.flatten.toSet
    val temp2 = makeUsersIndex(allUsers)
    val temp3 = makeUpperTriangularMatri(temp2,temp)
    println(temp3.deep.mkString("\n"))
    sc.stop()
  }





  }
