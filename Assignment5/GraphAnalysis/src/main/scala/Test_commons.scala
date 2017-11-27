package GraphAnalysis

import scala.collection.mutable.{Queue,HashMap,Set}
import scala.collection.immutable

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

  
  def addToSet(i:Int,j:Int,edges:HashMap[Int,Set[Int]])= {
    var temp = edges
    if (temp.contains(i)) temp(i) += j else temp += ((i,Set(j)))
    if (temp.contains(j)) temp(j) += i else temp += ((j,Set(i)))
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
    def runBFS(root:Int,nodes:immutable.Set[Int],edges:HashMap[Int,Set[Int]]) = {
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
}