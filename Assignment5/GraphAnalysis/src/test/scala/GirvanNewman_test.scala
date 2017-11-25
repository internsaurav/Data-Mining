package GraphAnalysis
import org.scalatest.FunSuite
import GirvanNewman._
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{IntegerType, StructType,StringType}
import org.graphframes.GraphFrame

import scala.collection.mutable

class GirvanNewman_test extends FunSuite{

  private def findIndexInTriangularMat(numUsers:Int, x:Int, y:Int):Int={
    var i = math.min(x,y)
    var j = math.max(x,y)
    var temp = ((i-1)*(numUsers-i.toFloat/2)+(j-i)).toInt
    println(s"found index for userIndex:${i},userIndex:${j} ====> ${temp}")
    temp
  }

  private def verifyTriangularmat(numUsers:Int): Unit ={
    val x = numUsers*numUsers/2
    println(s"As per existing implementation, size of array should be $x and indices range from 0 to ${x-1} ")
    println("Printing out the array indices used:")
    for (i <- 1 until numUsers){
      for (j<- i+1 to numUsers){
        val k = ((i-1)*(numUsers-i.toFloat/2)+(j-i)).toInt
        println(s"$i  $j  ==> $k ")
      }
    }
  }


  /* ratings1.csv
  * userId,movieId,rating,timestamp
      1,31,2.5,1260759144
      2,31,2.5,1260759144
      3,76,2.5,1260759144
      4,62,2.5,1260759144
      5,76,2.5,1260759144
      6,31,2.5,1260759144
      1,62,2.5,1260759144
  * */
//
  test("input1"){
    val ratingsFilePath = "../testInput/ratings1.csv"
    val sc = makeSparkContext()
    val (userSetForMovies,usersIndex) = extractGraphData(sc,ratingsFilePath)
    assert(userSetForMovies(31).size == 3)
    assert(userSetForMovies(31).contains(1))
    assert(userSetForMovies(31).contains(2))
    assert(userSetForMovies(31).contains(6))
    assert(userSetForMovies(62).size == 2)
    assert(userSetForMovies(62).contains(1))
    assert(userSetForMovies(62).contains(4))
    assert(userSetForMovies(76).size == 2)
    assert(userSetForMovies(76).contains(3))
    assert(userSetForMovies(76).contains(5))
    sc.stop()
  }

  test("triangularMatrix"){
    val ratingsFilePath = "../testInput/ratings1.csv"
    val sc = makeSparkContext()
    val (userSetForMovies,usersIndex) = extractGraphData(sc,ratingsFilePath)
    println(userSetForMovies.mkString("\n"))
    println(usersIndex.mkString("\n"))
    val countOfRatings:Array[Int] = makeUpperTriangularMatrix(usersIndex,userSetForMovies)
    println(countOfRatings.deep.mkString("\n"))
    val userIndex1=usersIndex(1)
    val userIndex2=usersIndex(2)
    val userIndex3=usersIndex(3)
    val userIndex4=usersIndex(4)
    val userIndex5=usersIndex(5)
    val userIndex6=usersIndex(6)
    assert(countOfRatings(findIndexInTriangularMat(6,userIndex1,userIndex2))==1)
    assert(countOfRatings(findIndexInTriangularMat(6,userIndex1,userIndex4))==1)
    assert(countOfRatings(findIndexInTriangularMat(6,userIndex3,userIndex5))==1)
    assert(countOfRatings(findIndexInTriangularMat(6,userIndex2,userIndex6))==1)
    assert(countOfRatings(findIndexInTriangularMat(6,userIndex3,userIndex5))==1)
    assert(countOfRatings(findIndexInTriangularMat(6,userIndex5,userIndex6))==0)
  }

  /*
  * userId,movieId,rating,timestamp
      1,31,2.5,1260759144
      2,31,2.5,1260759144
      3,76,2.5,1260759144
      4,62,2.5,1260759144
      5,76,2.5,1260759144
      6,31,2.5,1260759144
      1,62,2.5,1260759144
      2,62,2.5,1260759144
      1,75,2.5,1260759144
      2,75,2.5,1260759144
  * */
  test("triangularMatrix-input 2"){
    val ratingsFilePath = "../testInput/ratings2.csv"
    val sc = makeSparkContext()
    val (userSetForMovies,usersIndex) = extractGraphData(sc,ratingsFilePath)
    println(userSetForMovies.mkString("\n"))
    println(usersIndex.mkString("\n"))
    val countOfRatings:Array[Int] = makeUpperTriangularMatrix(usersIndex,userSetForMovies)
    println(countOfRatings.deep.mkString("\n"))
    val userIndex1=usersIndex(1)
    val userIndex2=usersIndex(2)
    assert(countOfRatings(findIndexInTriangularMat(6,userIndex1,userIndex2))==3)
  }

  test("triangularMatrix-actual input"){
    val ratingsFilePath = "../ratings.csv"
    val sc = makeSparkContext()
    val (userSetForMovies,usersIndex) = extractGraphData(sc,ratingsFilePath)
    println(userSetForMovies.mkString("\n"))
//    println(usersIndex.mkString("\n"))
    val countOfRatings:Array[Int] = makeUpperTriangularMatrix(usersIndex,userSetForMovies)
  }

  test("verifyTriangularmat"){
    verifyTriangularmat(5)
  }


  test("Creating Nodes dataframe"){
    val ratingsFilePath = "../ratings.csv"
    val sc = makeSparkContext()
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val (userSetForMovies,usersIndex) = extractGraphData(sc,ratingsFilePath)
    val countOfRatings:Array[Int] = makeUpperTriangularMatrix(usersIndex,userSetForMovies)
    val nodes = usersIndex.values.map(x => Row(x)).toSeq
    val nodeRDD = sc.parallelize(nodes)
    val struct = new StructType().add("id", IntegerType)
    val nodeDF = sqlContext.createDataFrame(nodeRDD,struct)
//    nodeDF.show()
    nodeDF.describe("id").show()
    sc.stop()
  }

  test("Creating Edge dataframe"){
    val ratingsFilePath = "../ratings.csv"
    val sc = makeSparkContext()
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val (userSetForMovies,usersIndex) = extractGraphData(sc,ratingsFilePath)
    val countOfRatings:Array[Int] = makeUpperTriangularMatrix(usersIndex,userSetForMovies)
    var edgeList = mutable.Set[Row]()
    val numUsers = usersIndex.values.size
    val indexUsers = usersIndex.map(_.swap)
    for(i <- 1 until numUsers){
      for(j <- i+1 to numUsers){
        val k = ((i-1)*(numUsers-i.toFloat/2)+(j-i)).toInt
        if(countOfRatings(k)>=3) {
//          val smallerNode = math.min(indexUsers(i),indexUsers(j))
//          val largerNode = math.max(indexUsers(i),indexUsers(j))
//          edgeList += (Row(smallerNode,largerNode))
          edgeList += (Row(i,j))
        }
      }
    }
    val struct = new StructType().add("src", IntegerType).add("dst",IntegerType)
    val edgeRDD = sc.parallelize(edgeList.toSeq)
    val edgeFrame = sqlContext.createDataFrame(edgeRDD,struct)
    edgeFrame.show()
    sc.stop()
  }

  test("Creating Graphframe"){
    val ratingsFilePath = "../ratings.csv"
    val sc = makeSparkContext()
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    val (userSetForMovies,usersIndex) = extractGraphData(sc,ratingsFilePath)
    val countOfRatings:Array[Int] = makeUpperTriangularMatrix(usersIndex,userSetForMovies)

    //nodes
    val nodes = usersIndex.values.map(x => Row(x)).toSeq
    val nodeRDD = sc.parallelize(nodes)
    val structNode = new StructType().add("id", IntegerType)
    val nodeDF = sqlContext.createDataFrame(nodeRDD,structNode)

    //edges
    val numUsers = usersIndex.values.size
//    val indexUsers = usersIndex.map(_.swap)
    val edgeFrame = createEdgeFrame(sc,numUsers,countOfRatings,3)
    val g = GraphFrame(nodeDF,edgeFrame)
    g.edges.show()
    g.vertices.show(672)
    sc.stop()
  }

  test("BFS"){
//    val ratingsFilePath = "../testInput/ratings1.csv"
    val sc = makeSparkContext()
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
//    val (userSetForMovies,usersIndex) = extractGraphData(sc,ratingsFilePath)
//    val countOfRatings:Array[Int] = makeUpperTriangularMatrix(usersIndex,userSetForMovies)

    //nodes
    val nodes = Seq(Row(1),Row(2))
    val nodeRDD = sc.parallelize(nodes)
    val structNode = new StructType().add("id", IntegerType)
    val nodeDF = sqlContext.createDataFrame(nodeRDD,structNode)

    //edges
    val numUsers = 2
    //    val indexUsers = usersIndex.map(_.swap)
//    val edgeFrame = createEdgeFrame(sc,numUsers,countOfRatings,3)
    val edgesRDD = sc.parallelize(Seq(Row(1,2),Row(2,1)))
    val structEdge = new StructType().add("src", IntegerType).add("dst",IntegerType)
    val edgeFrame = sqlContext.createDataFrame(edgesRDD,structEdge)
    val g = GraphFrame(nodeDF,edgeFrame)
    g.vertices.show()
    g.edges.show()
    g.bfs.fromExpr("id=2").toExpr("id=1").run().show()
    sc.stop()
  }

  test("Betweenness of small graph"){
    val sc = makeSparkContext()
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    //nodes
    val nodes = Seq(Row("A"),Row("B"),Row("C"),Row("D"),Row("E"),Row("F"),Row("G"))
    val nodeRDD = sc.parallelize(nodes)
    val structNode = new StructType().add("id", StringType)
    val nodeDF = sqlContext.createDataFrame(nodeRDD,structNode)
    //edges
    val edgesRDD = sc.parallelize(Seq(Row("A","B"),Row("B","A"),Row("B","C"),Row("C","B"),Row("A","C"),Row("C","A"),Row("B","D"),Row("D","B"),Row("E","D"),Row("D","E"),Row("G","D"),Row("D","G"),Row("F","D"),Row("D","F"),Row("F","E"),Row("E","F"),Row("F","G"),Row("G","F")))
    val structEdge = new StructType().add("src", StringType).add("dst",StringType)
    val edgeFrame = sqlContext.createDataFrame(edgesRDD,structEdge)
    val g = GraphFrame(nodeDF,edgeFrame)
//    g.vertices.show()
//    g.edges.show()
//    g.degrees.show()
    g.bfs.fromExpr("id='A'").toExpr("id").run().show()
    sc.stop()
  }



}
