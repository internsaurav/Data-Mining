package GraphAnalysis
import org.scalatest.FunSuite
import GirvanNewman._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.collection.mutable.{Queue,HashMap,Set,HashSet,Buffer}
import scala.collection.immutable
import Test_commons._
import org.apache.spark.broadcast.Broadcast

class Modularity_test extends FunSuite{

  test("Modularity calculation"){
    val ratingsFilePath = "../testInput/ratings4.csv"
    val sc = makeSparkContext()
    val (userSetForMovies,usersIndex) = extractGraphData(sc,ratingsFilePath)
    val nodes = userSetForMovies.values.flatten.toSet
    val countOfRatings:Array[Int] = makeUpperTriangularMatrix(usersIndex,userSetForMovies)
    val indexUsers = usersIndex.map(_.swap)
    val numUsers = indexUsers.keySet.max
    var edges = HashMap[Int,HashSet[Int]]()
    for(i <- 1 until numUsers){
        for(j<- i+1 to numUsers){
            val k = ((i-1)*(numUsers-i.toFloat/2)+(j-i)).toInt
            if(countOfRatings(k)>=1) {
                addToSet(indexUsers(i),indexUsers(j),edges)
            }
        }
    }
    val edgesBV = sc.broadcast(edges)
    var betweennessScores = sc.parallelize(usersIndex.keySet.toSeq).mapPartitions(roots => calculateBetweennessMR(roots,edgesBV)).reduceByKey((v1,v2)=>(v1+v2)).collectAsMap.mapValues(x=>x/2)
    var numEdges=0
    for((k,v) <-edges){
        numEdges += v.size
    }
    assert(betweennessScores.size == numEdges/2)
    var communities = findCommunitites(nodes,edges)
    var modularityMap = HashMap[Set[Int],(Int,Int)]()
    var m =sc.parallelize(edges.toSeq).map(data => (1,data._2.size)).reduceByKey((v1,v2)=>(v1+v2)).collect()(0)._2/2
    var degreesMap = sc.parallelize(edges.toSeq).map(data => (data._1,data._2.size)).collectAsMap()
    val mBV = sc.broadcast(m)
    val degreesMapBV = sc.broadcast(degreesMap)
    val aggFunction = (s:(Int,Int),v:(Int,Int)) => (s._1+v._1,s._2+v._2)
    val initialVal = (0,0)
    for (community <- communities){
        val nodePairs = community.subsets(2).toSeq
        val communityModularityData = sc.parallelize(nodePairs).map(x=>calculateAijKiKjforNodePair(x,edgesBV.value,degreesMapBV.value)).aggregate(initialVal)(aggFunction,aggFunction)
        modularityMap += ((community,communityModularityData))
    }
    edgesBV.destroy()
    mBV.destroy()
    degreesMapBV.destroy()
    // println(modularityMap)
    val modData=modularityMap.values.aggregate(initialVal)(aggFunction,aggFunction)
    var mod = ((modData._1/(2.0*m)-modData._2/(4.0*m*m)))
    // println(s"Modularity is $mod")
    // // println(s"Assessment of edges: ${numEdges/2}")
    
    // var mod = modularilty(sc,nodes,edges,m,degreesMap)
    // println(s"Modularity of the mother graph is $mod")
    var lastModularity = -1.0
    var modList=Buffer(mod)
    // var  res = ""
    // res += s"betweennessScores: ${betweennessScores.mkString(",")}"
    while(mod > lastModularity){
        val modifiedData = removeHighestBetweenessEdge(betweennessScores,edges,degreesMap)
        m -= 1
        betweennessScores = modifiedData._1
        edges = modifiedData._2
        degreesMap = modifiedData._3
        val removedEdge = modifiedData._4
        // res += s"Removed edge was ${removedEdge.deep.mkString(",")} \n"
        val parentCommunityOfEdge = findParentCommunityOfEdge(removedEdge,communities) //find the community where the edge was removed
        // res += s"Parent comm for the removed edge was $parentCommunityOfEdge \n"
        val subCommunities = findCommunitites(parentCommunityOfEdge.toSeq.toSet,edges) //check if the community got split
        // res += s"Subcommunitites: $subCommunities \n"
        if(subCommunities.size == 1){ //community not split
            // res += s"Not Split\n"
            val oldModularityData = modularityMap(parentCommunityOfEdge)
            val ki = degreesMap(removedEdge(0))
            val kj = degreesMap(removedEdge(1))
            val oldKi = ki+1
            val oldKj = kj+1
            val oldM = m+1
            val newAij = oldModularityData._1-1 //1 edge less in the community
            val reducedKiKjForNeighbours = (parentCommunityOfEdge &~ removedEdge.toSet).map(x=>degreesMap(x)*2).reduce((x,y)=>x+y)
            val newKiKj = oldModularityData._2 - oldKi*oldKj + ki*kj - reducedKiKjForNeighbours
            modularityMap(parentCommunityOfEdge) = (newAij,newKiKj) //update new values
            // res += s"oldKi: $oldKi , oldKj: $oldKj , oldM: $oldM, newAij:$newAij , newKiKj:$newKiKj  \n"
        } else {
            // res += s"Split\n"
            val edgesBV = sc.broadcast(edges)
            val degreesMapBV = sc.broadcast(degreesMap)
            modularityMap -= parentCommunityOfEdge //delete old values
            for (community <- subCommunities){
                val nodePairs = community.subsets(2).toSeq
                val communityModularityData = sc.parallelize(nodePairs).map(x=>calculateAijKiKjforNodePair(x,edgesBV.value,degreesMapBV.value)).aggregate(initialVal)(aggFunction,aggFunction)
                modularityMap += ((community,communityModularityData))
                communities += community
                // res += s"New community added: $community with Data: $communityModularityData\n"
            }
            edgesBV.destroy()
            degreesMapBV.destroy()
        }
        val modData=modularityMap.values.aggregate(initialVal)(aggFunction,aggFunction)
        // res += s"modData: $modData\n"
        lastModularity = mod
        mod = ((modData._1/(2.0*m)-modData._2/(4.0*m*m)))
        // res += s"mod: $mod\n"
        modList+=mod
    }
    sc.stop()
    println(s"Mod list is $modList")
    // println(// res)
  }

}
