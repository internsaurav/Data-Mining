package GraphAnalysis

import scala.collection.mutable.{Queue,HashMap,Set,HashSet}
import scala.collection.immutable
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

 object Test_commons {

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

  
  def addToSet(i:Int,j:Int,edges:HashMap[Int,HashSet[Int]])= {
    var temp = edges
    if (temp.contains(i)) temp(i) += j else temp += ((i,HashSet(j)))
    if (temp.contains(j)) temp(j) += i else temp += ((j,HashSet(i)))
    temp
  }

def findNodeName(node:Int):String={
        node match {
            case 1 => return "A"
            case 2 => return "B"
            case 3 => return "C"
            case 4 => return "D"
            case 5 => return "E"
            case 6 => return "F"
            case 7 => return "G"
            case 8 => return "Z" 
        }
    }

    def addParents(parent:Int,children:Set[Int],parentsMap:HashMap[Int,Set[Int]])={
        for(child<-children){
            if(parentsMap.contains(child)) parentsMap(child)+=parent else parentsMap += ((child,Set(parent)))
        }
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
    def runBFS(root:Int,edges:HashMap[Int,HashSet[Int]]) = {
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
        (bfsMap,parentsMap.map(x=>(x._1,x._2.toStream.toSet)))
    }

    /*
    * nodeScores - variable representing the scores in betweenness calculation according to GN algorithm. The nodeScores are passed on to incoming edges. \
                    All the nodes have a starting value of 1.
    * edgeScores- hashMap that stores the betweenness scores for each edge.
    * numLevels - total levels of nodes in this BFS Tree. Root is level 0.
    * Traversing the  BFS tree bottom up. Take the nodes at each level. Take each node. Find its parents. \ 
      Distribute its nodeScore among its parents(add to the parents scores).
    * Assign these scores to the edges joining there parents.
    */
  def betweennessScore(bfsMap : HashMap[Int,immutable.Set[Int]],parentsMaps:HashMap[Int,immutable.Set[Int]])={
    val nodeScores = HashMap[Int,Float]().withDefaultValue(1f)
    val edgeScores = HashMap[immutable.Set[Int],Float]()
    val numLevels = bfsMap.keySet.max
    // println(s"numLevels: $numLevels")
    for(level <- numLevels until 0 by -1){
        // println(s"Level: $level")
        val thisLevelNodes = bfsMap(level)
        // println(thisLevelNodes)
        for (node <- thisLevelNodes){
            val parents = parentsMaps(node)
            // println(parents)
            val nodeScore = nodeScores(node)
            val nodeScorePerParent = nodeScore/parents.size
            parents.foreach(parent => (nodeScores(parent)+=nodeScorePerParent))
            parents.foreach(parent => (edgeScores(immutable.Set(node,parent)) = nodeScorePerParent))
        }
    }
    edgeScores
  }

  /*
    *frontier - the main queue which holds the nodes to be visited next
    *visitedNodes- set of nodes already visited in this traversal
    *distance - the level
    *thisLevelNodes -  nodes in a particular level
    *children- Children of the node. First finds the set of neighbours and removes the already visited nodes
    */
    def connectedComponents(root:Int,edges:HashMap[Int,HashSet[Int]]) = {
        val frontier = Queue[Int]()
        val visitedNodes = HashSet[Int]()
        frontier.enqueue(root)
        while(!frontier.isEmpty){
            // println(s"frontier ====> ${frontier.mkString(",")}")
            val thisLevelNodes = frontier.dequeueAll(x=>true)
            visitedNodes ++= thisLevelNodes
            val thisLevelNeighbours = Set[Set[Int]]()
            for(thisNode <- thisLevelNodes){
                val children = edges(thisNode)-- visitedNodes
                thisLevelNeighbours += children
            }
            frontier ++= thisLevelNeighbours.flatten
        }
        // println(bfsMap.mapValues(x=>x.map(y=>findNodeName(y))).mkString("\n"))
        // println(s"parents map ===> ${parentsMap.map(x =>(findNodeName(x._1),x._2.map(y=>findNodeName(y)))).mkString(",")}")
        visitedNodes
    }

    /*
    nodesToBeVisited - set of all the nodes in the graph. ALl of these nodes have to be visited. \
                    HashSet selected here as we delete nodes by searching hence we need constant time operations.
    communities - self-explanatory. The keys are meaningless here.
    randomNode - any node from where we can start a BFS.
    connectedComps - connected components in the current community where we ran the BFS.
    Qx2m - Q * 2m
    Approach - Repeat till no nodes left 
                Run BFS, find the connected components, remove the same from list if nodes to be visited.
        After finding all the communities, take each community, take each pair of nodes, \ 
        calculate their degrees etc
        Q = 1/2m * overall communitites -> over all edges -> (Aij - kikj/2m) \ 
         where Aij =1 if there is an edge 0 otherwise 
  */
  def modularilty(sc:SparkContext ,nodes:immutable.Set[Int], edges:HashMap[Int,HashSet[Int]],m:Int, degreesMap:scala.collection.Map[Int,Int])={
    val nodesToBeVisited = HashSet[Int]()
    val communities = Set[HashSet[Int]]()

    nodes.foreach(x => (nodesToBeVisited += x))
    while(!nodesToBeVisited.isEmpty){
        val randomNode = nodesToBeVisited.head
        val connectedComps = connectedComponents(randomNode,edges)
        communities += connectedComps
        nodesToBeVisited --= connectedComps
    }


    // println(s"m calculated : ${m}")
    val edgesBV = sc.broadcast(edges)
    val mBV = sc.broadcast(m)
    val degreesMapBV = sc.broadcast(degreesMap)
    val modularity = sc.parallelize(communities.toSeq).mapPartitions(communititesItr =>calculateQinPartition(communititesItr,edgesBV,mBV,degreesMapBV)).aggregate(0.0)((x1,y1)=>(x1+y1),(x2,y2)=>(x2+y2))/(2*m)
    edgesBV.destroy()
    mBV.destroy()
    degreesMapBV.destroy()
    (modularity,communities.size)
  }

  def calculateQinPartition(communities:Iterator[HashSet[Int]],edgesBV:Broadcast[HashMap[Int,HashSet[Int]]],mBV:Broadcast[Int],degreesMapBV:Broadcast[scala.collection.Map[Int,Int]])={
    var Qx2m = 0.0
    communities.foreach(community => (Qx2m += calculateQforCommunity(community,edgesBV.value,mBV.value,degreesMapBV.value)))
    Iterator(Qx2m)
  }

def calculateQforCommunity(community:HashSet[Int],edges:HashMap[Int,HashSet[Int]],m:Int,degreesMap:scala.collection.Map[Int,Int])={
    var Qx2m = 0.0
    community.subsets(2).foreach(nodePair => (Qx2m += calculateQforNodePair(nodePair,edges,m,degreesMap)))
    Qx2m
  }

  def calculateQforNodePair(data:Set[Int],edges:HashMap[Int,HashSet[Int]],m:Int,degreesMap:scala.collection.Map[Int,Int])={
    val ij = data.toArray
    val i = ij(0)
    val j = ij(1)
    val Aij = if (edges.contains(i) && edges(i).contains(j)) 1 else 0
    val ki = degreesMap.getOrElse(i,0)
    val kj = degreesMap.getOrElse(j,0)
    Aij - (ki*kj)/(2.0*m)
  }

  def calculateAijKiKjforNodePair(data:Set[Int],edges:HashMap[Int,HashSet[Int]],degreesMap:scala.collection.Map[Int,Int])={
    val ij = data.toArray
    val i = ij(0)
    val j = ij(1)
    val Aij = if (edges.contains(i) && edges(i).contains(j)) 1 else 0
    val ki = degreesMap.getOrElse(i,0)
    val kj = degreesMap.getOrElse(j,0)
    (Aij*2.0,ki*kj*2.0)
  }

  def removeHighestBetweenessEdge(betweennessScores:scala.collection.Map[immutable.Set[Int],Float],edges:HashMap[Int,HashSet[Int]],degreesMap:scala.collection.Map[Int,Int])={
    val (edgeSet,maxVal) = betweennessScores.maxBy(_._2)
    var newBetweennessScores = betweennessScores - edgeSet
    val newEdges=edges
    val edgeArr = edgeSet.toArray
    newEdges(edgeArr(0))-=edgeArr(1)
    newEdges(edgeArr(1))-=edgeArr(0)
    val degreeI = degreesMap(edgeArr(0))-1
    val degreeJ = degreesMap(edgeArr(1))-1
    var newDegreesMap = degreesMap - edgeArr(0) - edgeArr(1)
    newDegreesMap += ((edgeArr(0),degreeI))
    newDegreesMap += ((edgeArr(1),degreeJ))
    (newBetweennessScores,newEdges,newDegreesMap,edgeArr)
  }

  def findCommunitites(nodes:immutable.Set[Int],edges:HashMap[Int,HashSet[Int]])={
    val nodesToBeVisited = HashSet[Int]()
    val communities = Set[HashSet[Int]]()

    nodes.foreach(x => (nodesToBeVisited += x))
    while(!nodesToBeVisited.isEmpty){
        val randomNode = nodesToBeVisited.head
        val connectedComps = connectedComponents(randomNode,edges)
        communities += connectedComps
        nodesToBeVisited --= connectedComps
    }
    communities
  }

  def findParentCommunityOfEdge(removedEdge:Array[Int],communities:Set[HashSet[Int]])={
    var parentCommunity = HashSet[Int]()
    var communityFound = false
    val itr = communities.toIterator
    while(!communityFound){
        val tempCOmmunity = itr.next
        if(tempCOmmunity.contains(removedEdge(0)) || tempCOmmunity.contains(removedEdge(1))){
            communityFound = true
            parentCommunity = tempCOmmunity
        }
    }
    parentCommunity
  }

  def addLastRemovedEdge(lastRemovedEdge:Array[Int],edges:HashMap[Int,HashSet[Int]]) = {
    var finalEdges = edges
    finalEdges(lastRemovedEdge(0)) += lastRemovedEdge(1)
    finalEdges(lastRemovedEdge(1)) += lastRemovedEdge(0)
    finalEdges
  }
}