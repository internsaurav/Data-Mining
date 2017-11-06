
import java.lang.AssertionError

import org.scalatest.FunSuite
import sahu_saurav_clustering._
import scala.collection.mutable

class sahu_saurav_clusteringTest extends FunSuite {

  val (clusters,labelsMap) = processDataForClustering("test")
  test("Number of clusters should be 150"){
    assert(clusters.size == 150)
    assert(labelsMap.size == 150)
  }

  test("All values from 0 to 149 should be present in both maps"){
    for (i<- 0 until 150){
      assert(clusters.contains(i))
      val cluster = clusters(i)
      assert(labelsMap.contains(i))
    }
  }

  test("Key 150 should not be present"){
      assert(!clusters.contains(150))
  }

  //test for findCentroidOfCluster
  test("test for findCentroidOfCluster"){
    var cluster = Array(0f,0f,0f,0f,1f)
    var centroid = findCentroidOfCluster(cluster)
    assert(centroid.deep == Array(0f,0f,0f,0f).deep)

    cluster = Array(1f,2f,3f,4f,1f)
    centroid = findCentroidOfCluster(cluster)
    assert(centroid.deep == Array(1f,2f,3f,4f).deep)

    cluster = Array(1f,2f,3f,4f,2f)
    centroid = findCentroidOfCluster(cluster)
    assert(centroid.deep == Array(0.5f,1f,1.5f,2f).deep)

    cluster = Array(1f,2f,3f,4f,4f)
    centroid = findCentroidOfCluster(cluster)
    assert(centroid.deep == Array(0.25f,0.5f,0.75f,1f).deep)

    cluster = Array(1f,2f,3f,4f,4f)
    centroid = findCentroidOfCluster(cluster)
    assert(centroid.deep == Array(0.25f,0.5f,0.75f,1.0f).deep)

  }
  //9===>4.9,3.1,1.5,0.1,1.0
  //34===>4.9,3.1,1.5,0.1,1.0
  //37===>4.9,3.1,1.5,0.1,1.0


  //test for euclideanDistance
  test("test for euclideanDistance"){
    var pointA = Array(0f,0f,0f,0f)
    var pointB = Array(0f,0f,0f,0f)
    assert(euclideanDistance(pointA,pointB) == 0f)

    assert(euclideanDistance(Array(5.8f,2.7f,5.1f,1.9f),Array(5.8f,2.7f,5.1f,1.9f)) == 0f)

    pointA = Array(0f,0f,0f,0f)
    pointB = Array(1f,1f,1f,1f)
    assert(euclideanDistance(pointA,pointB) == 2f)

    assert(euclideanDistance(pointA,pointB) == euclideanDistance(pointB,pointA))

    pointA = Array(0f,0f,0f,0f)
    pointB = Array(0f,1f,1f,1f)
    assert(euclideanDistance(pointA,pointB) == Math.sqrt(3).toFloat)

    pointA = Array(0f,1f,0f,0f)
    pointB = Array(0f,1f,1f,1f)
    assert(euclideanDistance(pointA,pointB) == Math.sqrt(2).toFloat)

    pointA = Array(-1f,1f,0f,25f)
    pointB = Array(0f,1f,-15.2f,1f)
    assert(euclideanDistance(pointA,pointB) == Math.sqrt(808.04).toFloat)

    pointA = Array(-100f,100f,45000f,0.00008f)
    pointB = Array(10f,1f,-15.2f,1f)
    val dis = euclideanDistance(pointA,pointB)
    val comp = Math.sqrt(2026390130).toFloat
    println(dis)
    println(comp)
    assert(dis == comp)
  }


  test("priority queue"){
    val clustersPriorityQueue = new mutable.PriorityQueue[(Int,Int,Float)]()(Ordering.by(-_._3))
    clustersPriorityQueue.enqueue((1,1,0f))
    clustersPriorityQueue.enqueue((1,1,5f))
    clustersPriorityQueue.enqueue((1,1,3f))
    clustersPriorityQueue.enqueue((1,1,18f))
    clustersPriorityQueue.enqueue((1,1,-1f))

    while(clustersPriorityQueue.nonEmpty){
      println(clustersPriorityQueue.dequeue())
    }

  }

  test("clusters"){
    for((k,v)<- clusters){
      print(k+"===>")
      println(v.deep.mkString(","))
    }
  }

  test("makePQ"){
    val priorityQueueProxy = makePriorityQueue(clusters)
    while (priorityQueueProxy.nonEmpty){
      println(priorityQueueProxy.dequeue())
    }
  }

  test("byRefOrByVal"){
    var x= Array(1,2,3,4)
    println(x.deep.mkString(","))
    process(x)
    println(x.deep.mkString(","))

    var y = mutable.HashMap(1->1,2->1)
    println(y.mkString(","))
    process2(y)
    println(y.mkString(","))
  }

    def process(x: Array[Int])={
      x(0) = 1000
    }

  def process2(y: mutable.HashMap[Int, Int])={
    y(1) = 1000
    y -= 2
  }

  test("mergeClusters"){
    val x = Array(0f,1f,2f,3f,4f)
    val z = Array(1f,2f,3f,4f,5f)
    update(x,z)
    assert(x.deep == Array(1f,3f,5f,7f,9f).deep)

    val priorityQueueProxy = makePriorityQueue(clusters)
    var temp = priorityQueueProxy.clone()
    println(temp.mkString(","))
    temp = removeEntriesForMergedClusters(temp,50,100)
    println(temp.mkString(","))
  }

  test("finalClustering"){
    val priorityQueue = makePriorityQueue(clusters)
    var copyOfClusters = clusters.clone()
    val finalClusters = doHierarchicalClustering(copyOfClusters,priorityQueue,3)
    assert(clusters.values.toArray.deep != copyOfClusters.values.toArray.deep)
    var numPoints = 0
    for (cluster <- finalClusters){
      numPoints += cluster._2.size+1
    }
    assert(numPoints == 150)
  }

  test("findlabels"){
    val priorityQueue = makePriorityQueue(clusters)
    var copyOfClusters = clusters.clone()
    val finalClusters = doHierarchicalClustering(copyOfClusters,priorityQueue,3)
    val labels = applyLabels(finalClusters,clusters,labelsMap)
    println(labels)
  }

  test("cloneMap"){
    var map1 = mutable.HashMap[Int,Array[Float]]((1,Array(0f,1f)))
    map1.foreach(x=>println(x._2.deep.mkString(", ")))
    var map2 = map1.clone()
//    map2 -= 1 //works fine does not affect
//    map1.foreach(x=>println(x._2.deep.mkString(", ")))
//    map2.foreach(x=>println(x._2.deep.mkString(", ")))
    var x = map2(1).clone()
    x(0)=1f
    map2(1) = x
    map1.foreach(x=>println(x._2.deep.mkString(", ")))
    map2.foreach(x=>println(x._2.deep.mkString(", ")))
  }

  test("everything"){
    for (i<- 1 to 150){
      sahu_saurav_clustering.main(Array("test",i.toString))
    }

  }


}
