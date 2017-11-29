// package GraphAnalysis
// import org.scalatest.FunSuite
// import GirvanNewman._
// import org.apache.spark.SparkContext
// import org.apache.spark.sql.{DataFrame, Row}
// import org.apache.spark.sql.types.{IntegerType, StructType,StringType}
// import org.graphframes.GraphFrame

// import scala.collection.mutable

// class Betweenness_test extends FunSuite{

// //   test("Betweenness of small graph"){
// //     val sc = makeSparkContext()
// //     val sqlContext = new org.apache.spark.sql.SQLContext(sc)
// //     //nodes
// //     val nodes = Seq(Row("A"),Row("B"),Row("C"),Row("D"),Row("E"),Row("F"),Row("G"))
// //     val nodeRDD = sc.parallelize(nodes)
// //     val structNode = new StructType().add("id", StringType)
// //     val nodeDF = sqlContext.createDataFrame(nodeRDD,structNode)
// //     //edges
// //     val edgesRDD = sc.parallelize(Seq(Row("A","B"),Row("B","A"),Row("B","C"),Row("C","B"),Row("A","C"),Row("C","A"),Row("B","D"),Row("D","B"),Row("E","D"),Row("D","E"),Row("G","D"),Row("D","G"),Row("F","D"),Row("D","F"),Row("F","E"),Row("E","F"),Row("F","G"),Row("G","F")))
// //     val structEdge = new StructType().add("src", StringType).add("dst",StringType)
// //     val edgeFrame = sqlContext.createDataFrame(edgesRDD,structEdge)
// //     val g = GraphFrame(nodeDF,edgeFrame)
// //     var vertices = g.vertices.select("id").collect().map(x=>x.getAs[String](0)).toSeq
// //     println(vertices)
// //     g.shortestPaths.landmarks(vertices).run().show()

// //    // g.edges.show()
// //    // g.degrees.show()
// // //    g.bfs.fromExpr("id='E'").toExpr("id != 'E'").run().show()
// //     sc.stop()
// //   }
// test("Betweenness of large graph"){
//      val ratingsFilePath = "../ratings.csv"
//     val sc = makeSparkContext()
//     val sqlContext = new org.apache.spark.sql.SQLContext(sc)
//     val (userSetForMovies,usersIndex) = extractGraphData(sc,ratingsFilePath)
//     val countOfRatings:Array[Int] = makeUpperTriangularMatrix(usersIndex,userSetForMovies)

//     //nodes
//     val nodes = usersIndex.values.map(x => Row(x)).toSeq
//     val nodeRDD = sc.parallelize(nodes)
//     val structNode = new StructType().add("id", IntegerType)
//     val nodeDF = sqlContext.createDataFrame(nodeRDD,structNode)

//     //edges
//     val numUsers = usersIndex.values.size
// //    val indexUsers = usersIndex.map(_.swap)
//     val edgeFrame = createEdgeFrame(sc,numUsers,countOfRatings,3)
//     val g = GraphFrame(nodeDF,edgeFrame)
//     var vertices = g.vertices.select("id").collect().map(x=>x.getAs[Int](0)).toSeq
//     g.shortestPaths.landmarks(vertices).run().show()

//    // g.edges.show()
//    // g.degrees.show()
// //    g.bfs.fromExpr("id='E'").toExpr("id != 'E'").run().show()
//     sc.stop()
//   }


// }
