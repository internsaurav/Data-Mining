
import java.lang.AssertionError

import org.scalatest.FunSuite
import sahu_saurav_clustering.{processDataForClustering,findCentroidOfCluster,euclideanDistance}

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
      assert(labelsMap.contains(cluster))
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

  //test for euclideanDistance
  test("test for euclideanDistance"){
    var pointA = Array(0f,0f,0f,0f)
    var pointB = Array(0f,0f,0f,0f)
    assert(euclideanDistance(pointA,pointB) == 0f)

    pointA = Array(0f,0f,0f,0f)
    pointB = Array(1f,1f,1f,1f)
    assert(euclideanDistance(pointA,pointB) == 2f)

    assert(euclideanDistance(pointA,pointB) == euclideanDistance(pointB,pointA))


  }

}
