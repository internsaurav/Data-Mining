// package GraphAnalysis
// import org.scalatest.FunSuite
// import GirvanNewman._
// import org.apache.spark.SparkContext
// import scala.collection.mutable.{Queue,HashMap,Set}
// import scala.collection.immutable
// import Test_commons._

// class Betweenness_calculation extends FunSuite{

//   test("Betweenness_calculation"){
//     val ratingsFilePath = "../ratings.csv"
//     val sc = makeSparkContext()
//     val (userSetForMovies,usersIndex) = extractGraphData(sc,ratingsFilePath)
//     val nodes = userSetForMovies.values.flatten.toSet
//     // println(nodes.mkString("\n"))
//     // println(usersIndex.mkString("\n"))
//     // println(userSetForMovies.mkString("\n"))
//     val countOfRatings:Array[Int] = makeUpperTriangularMatrix(usersIndex,userSetForMovies)
//     val indexUsers = usersIndex.map(_.swap)
//     val numUsers = indexUsers.keySet.max
//     // println(numUsers)
//     val edges = HashMap[Int,Set[Int]]()
//     for(i <- 1 until numUsers){
//         for(j<- i+1 to numUsers){
//             val k = ((i-1)*(numUsers-i.toFloat/2)+(j-i)).toInt
//             if(countOfRatings(k)>=1) {
//                 addToSet(indexUsers(i),indexUsers(j),edges)
//             }
//         }
//     }
//     val edgesBV = sc.broadcast(edges)
//     val betweennessScores = sc.parallelize(usersIndex.keySet.toSeq).mapPartitions(roots => calculateBetweennessMR(roots,edgesBV)).reduceByKey((v1,v2)=>(v1+v2)).collectAsMap.mapValues(x=>x/2)
//     edgesBV.destroy()
//     sc.stop()
//     var numEdges=0
//     for((k,v) <-edges){
//         numEdges += v.size
//     }
//     assert(betweennessScores.size == numEdges/2)
    
//   }

  
// }
