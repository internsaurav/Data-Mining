import scala.collection.mutable
import scala.io.Source
import scala.math

object sahu_saurav_clustering {

  def main(args: Array[String]):Unit= {

    val testfilePath = args(0)
    val (clusters,labelsMap) = processDataForClustering(testfilePath)
    makePriorityQueue(clusters)
  }

  /*
  * clusters- This is a map of cluster-number to cluster. Every point is a cluster in itself initially.
  * Each cluster is represented by 5 values (all are in floats) -
  *   1) sum of sepal lengths in the cluster
  *   2) sum of sepal widths in the cluster
  *   3) sum of petal lengths in the cluster
  *   4) sum of petal widths in the cluster
  *   5) number of points in the cluster
  *   Initially 1)-4) values are just the actual lengths and widths of sepals and petals of individual species
  *   Initially value of 5) is 1.
  *   As the cluster grows, the values for each member of cluster is added. Number of points is incremented accordingly.
  *
  * labelsMap- This maps clusters to its labels as provided in input data
  * */
  def processDataForClustering(testfilePath: String):(mutable.HashMap[Int,Array[Float]],mutable.HashMap[Array[Float],String]) = {

    val clusters = new mutable.HashMap[Int,Array[Float]]()
    val labelsMap = new mutable.HashMap[Array[Float],String]()
    var clusterCounter = 0
    for (line <- Source.fromFile(testfilePath).getLines()){
      if (line.trim != ""){
        val cluster = new Array[Float](5)
        val data = line.split(",")
        val sepalLength = data(0).toFloat
        val sepalWidth = data(1).toFloat
        val petalLength = data(2).toFloat
        val petalWidth = data(3).toFloat
        val label = data(4)
        val initialClusterPopulation = 1.0f
        cluster(0) = sepalLength
        cluster(1) = sepalWidth
        cluster(2) = petalLength
        cluster(3) = petalWidth
        cluster(4) = initialClusterPopulation
        clusters += ((clusterCounter,cluster))
        labelsMap += ((cluster,label))
        clusterCounter += 1
      }
    }
    (clusters,labelsMap)

  }

  /*
  * finds the euclidean distance between clusters
  * the method first finds the centroid of the clusters
  * then the distance between centroids is calculated
  * */
  def euclideanDistanceBetweenClusters(clusterA:Array[Float], clusterB:Array[Float]) = {
    val centroidA = findCentroidOfCluster(clusterA)
    val centroidB = findCentroidOfCluster(clusterB)

  }

  //calculates the centroid of a cluster
  def findCentroidOfCluster(cluster:Array[Float]): Array[Float] = {

    val centroid = new Array[Float](4)
    val clusterPopulation = cluster(4)
    for (i <-0 until 4){
      centroid(i) = cluster(i)/clusterPopulation
    }
    centroid

  }

  //calculates the euclidean distance
  def euclideanDistance(pointA:Array[Float], pointB:Array[Float]): Float = {

    var sumOfSquares = 0.0
    for (i<-0 until 4){
      sumOfSquares += Math.pow((pointA(i)-pointB(i)),2)
    }
    math.sqrt(sumOfSquares).toFloat

  }

  def makePriorityQueue(intToFloats: mutable.HashMap[Int, Array[Float]])={

  }
}
