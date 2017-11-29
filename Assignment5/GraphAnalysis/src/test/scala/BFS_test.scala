// package GraphAnalysis
// import org.scalatest.FunSuite
// import GirvanNewman._
// import org.apache.spark.SparkContext
// import scala.collection.mutable.{Queue,HashMap,Set}
// import scala.collection.immutable
// import Test_commons._

// class BFS_test extends FunSuite{

//   test("BFS_test"){
//     val ratingsFilePath = "../testInput/ratings4.csv"
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
//     // println(edges.mkString("\n"))
//     val bfsMaps = HashMap[Int,HashMap[Int,immutable.Set[Int]]]()
//     val parentsMaps = HashMap[Int,HashMap[Int,Set[Int]]]()
//     for (i <- 1 to 7){
//         // println(s"running BFS from Node $i")
//         val (bfsMap,parentsMap) = runBFS(i,nodes,edges)
//         bfsMaps += ((i,bfsMap))
//         parentsMaps += ((i,parentsMap))
//         // println("============================")
//     }
//     println("BFSMaps:")
//     println(bfsMaps.mkString("\n"))
//     println("parentsMaps:")
//     println(parentsMaps.mkString("\n"))
//     sc.stop()
//   }
    
//     *frontier - the main queue which holds the nodes to be visited next
//     *bfsMap - the BFS tree, containing the nodes at level-wie distance from root
//     *parentsMap- hashMap containing the parents of each node in this BFS tree
//     *visitedNodes- set of nodes already visited in this traversal
//     *distance - the level
//     *thisLevelNodes -  nodes in a particular level
//     *children- Children of the node. First finds the set of neighbours and removes the already visited nodes
    
//     def runBFS(root:Int,nodes:immutable.Set[Int],edges:HashMap[Int,Set[Int]]) = {
//         val frontier = Queue[Int]()
//         val bfsMap = HashMap[Int,immutable.Set[Int]]()
//         val parentsMap = HashMap[Int,Set[Int]]()
//         val visitedNodes = Set[Int]()
//         var distance = 0
//         frontier.enqueue(root)
//         while(!frontier.isEmpty){
//             // println(s"frontier ====> ${frontier.mkString(",")}")
//             val thisLevelNodes = frontier.dequeueAll(x=>true)
//             visitedNodes ++= thisLevelNodes
//             bfsMap += ((distance,thisLevelNodes.toSet))
//             val thisLevelNeighbours = Set[Set[Int]]()
//             for(thisNode <- thisLevelNodes){
//                 val children = edges(thisNode)-- visitedNodes
//                 addParents(thisNode,children,parentsMap)
//                 thisLevelNeighbours += children
//             }
//             frontier ++= thisLevelNeighbours.flatten
//             distance += 1
//         }
//         // println(bfsMap.mapValues(x=>x.map(y=>findNodeName(y))).mkString("\n"))
//         // println(s"parents map ===> ${parentsMap.map(x =>(findNodeName(x._1),x._2.map(y=>findNodeName(y)))).mkString(",")}")
//         (bfsMap,parentsMap)
//     }
// }
