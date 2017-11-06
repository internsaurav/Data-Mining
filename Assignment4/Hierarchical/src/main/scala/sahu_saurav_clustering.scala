import java.io.{File, PrintWriter}

import scala.collection.mutable.ListBuffer
import scala.collection.{immutable, mutable}
import scala.io.Source
import scala.math

object sahu_saurav_clustering {

  def main(args: Array[String]):Unit= {

    val testfilePath = args(0)
    val targetNumCLusters = args(1).toInt
    val outputFileName = s"saurav_sahu_${targetNumCLusters}.txt"
    val (clusters,labelsMap) = processDataForClustering(testfilePath)
    val priorityQueue = makePriorityQueue(clusters)
    var copyOfClusters = clusters.clone()
    val finalClusters = doHierarchicalClustering(copyOfClusters,priorityQueue,targetNumCLusters)
    val labels = applyLabels(finalClusters,clusters,labelsMap)
    handleOutput(finalClusters,labels,clusters,labelsMap,outputFileName)
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
  def processDataForClustering(testfilePath: String):(mutable.HashMap[Int,Array[Float]],mutable.HashMap[Int,String]) = {

    val clusters = new mutable.HashMap[Int,Array[Float]]()
    val labelsMap = new mutable.HashMap[Int,String]()
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
        labelsMap += ((clusterCounter,label))
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
  def euclideanDistanceBetweenClusters (clusterA:Array[Float], clusterB:Array[Float])  = {

    val centroidA = findCentroidOfCluster(clusterA)
    val centroidB = findCentroidOfCluster(clusterB)
    euclideanDistance(centroidA, centroidB)

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

  /*
  * Builds the initial priority queue
  * At this stage, each point is cluster in itself.
  * */
  def makePriorityQueue(clusters: mutable.HashMap[Int, Array[Float]]):mutable.PriorityQueue[(Int,Int,Float)]={

    val pairsOfPoints = clusters.keySet.subsets(2)
    var clustersPriorityQueue = new mutable.PriorityQueue[(Int,Int,Float)]()(Ordering.by(-_._3))
    while(pairsOfPoints.hasNext){
      val pairOfPoints = pairsOfPoints.next().toArray
      val clusterA = pairOfPoints(0)
      val clusterB = pairOfPoints(1)
      val distance = euclideanDistanceBetweenClusters(clusters(clusterA),clusters(clusterB))
      clustersPriorityQueue.enqueue((clusterA,clusterB,distance))
    }
    clustersPriorityQueue

  }

  /*
  * Applies hierarchical clustering to the clusters.
  * clusterStats - the summary of clusters (sum of each dimension and number of points)
  * clusters -  the actual cluster data, i.e. details points in each cluster
  * Uses the priority queue built initially when each point is a clusters in its own
  * Algorithm-
  * When we decide to merge two clusters C and D, we remove all entries
    in the priority queue involving one of these two clusters; that requires
    work O(n log n) since there are at most 2n deletions to be performed, and
    priority-queue deletion can be performed in O(log n) time.
    We then compute all the distances between the new cluster and the re-
    maining clusters. This work is also O(n log n), as there are at most n
    entries to be inserted into the priority queue, and insertion into a priority
    queue can also be done in O(log n) time.
  * currentNumClusters - initial number of clusters to begin with. This is the size of original data points
  * */
  def doHierarchicalClustering(clusterStats: mutable.HashMap[Int, Array[Float]], clustersPriorityQueue: mutable.PriorityQueue[(Int, Int, Float)], targetNumClusters:Int) = {

    val clusters = mutable.HashMap[Int,mutable.Set[Int]]()
    var currentNumClusters = clusterStats.size
    var temp = clustersPriorityQueue.clone()
    val tempMergeList = new ListBuffer[String]()
    while (currentNumClusters > targetNumClusters){
      temp = mergeClusters(clusterStats,temp,clusters,tempMergeList)
      currentNumClusters -= 1
    }
    mergeOutliers(clusters,clusterStats)
    writeDendro(tempMergeList)
    clusters
  }

  /*
  * Merges 2 clusters, removes their entries from Priority Queue, inserts new entries with updated values.
  * Merge Rule -  cluster with bigger cluster index merges into the one with smaller cluster index.
  * */
  def mergeClusters(clusterStats: mutable.HashMap[Int, Array[Float]], clustersPriorityQueue: mutable.PriorityQueue[(Int, Int, Float)], clusters:mutable.HashMap[Int,mutable.Set[Int]],tempMergeList :mutable.ListBuffer[String])={
    val (clusterA,clusterB) = merge(clusterStats,clustersPriorityQueue,clusters)
    tempMergeList += (clusterA.toString+" <--- " + clusterB.toString)
    var temp = clustersPriorityQueue.clone()
    temp = removeEntriesForMergedClusters(temp,clusterA,clusterB)
    insertEntriesForNewCluster(temp,clusterStats,clusterA)
    temp
  }

  /*
  * merges 2 clusters
  * clusterB gets merged into clusterA and loses its identity, so we delete the data of clusterB from the 'clusters' map
  * updates data for clusterA
  * returns clusterA and clusterB
  * */
  def merge(clusterStats: mutable.HashMap[Int, Array[Float]], clustersPriorityQueue: mutable.PriorityQueue[(Int, Int, Float)],clusters:mutable.HashMap[Int,mutable.Set[Int]])={

    val clustersToMerge = clustersPriorityQueue.dequeue()
    val clusterA = Math.min(clustersToMerge._1,clustersToMerge._2)
    val clusterB = Math.max(clustersToMerge._1,clustersToMerge._2)
    val clusterAdata = clusterStats(clusterA)
    val clusterBdata = clusterStats(clusterB)
    clusterStats -= clusterB
    val updateClusterAdata = update(clusterAdata,clusterBdata)
    var actualPointsInClusterA = if (clusters.contains(clusterA)) clusters(clusterA) else mutable.Set[Int]()
    var actualPointsInClusterB = if (clusters.contains(clusterB)) clusters(clusterB) else mutable.Set[Int]()
    actualPointsInClusterA ++= actualPointsInClusterB
    actualPointsInClusterA += clusterB
    clusters(clusterA) = actualPointsInClusterA
    clusters -= clusterB
    clusterStats(clusterA) = updateClusterAdata
    (clusterA,clusterB)
  }

  // updates data of clusterAdata with clusterBdata
  def update(clusterAdata: Array[Float], clusterBdata: Array[Float]) = {
    val temp = clusterAdata.clone()
    for (i <- 0 until temp.length){
      temp(i) += clusterBdata(i)
    }
    temp
  }

  //removes the entries which contain one of the merged clusters
  def removeEntriesForMergedClusters(clustersPriorityQueue: mutable.PriorityQueue[(Int, Int, Float)], clusterA: Int, clusterB: Int) = {
    clustersPriorityQueue.filter(x => (!(immutable.HashSet(clusterA,clusterB).contains(x._1) || immutable.HashSet(clusterA,clusterB).contains(x._2))))
  }

  //inserts entries for new cluster
  def insertEntriesForNewCluster(priorityQueue: mutable.PriorityQueue[(Int, Int, Float)], clusterStats: mutable.HashMap[Int, Array[Float]], clusterA: Int) = {
    val clusterIndices = clusterStats.keySet - clusterA
    val clusterAdata = clusterStats(clusterA)
    for (clusterB <- clusterIndices){
      val clusterBData = clusterStats(clusterB)
      val distance = euclideanDistanceBetweenClusters(clusterAdata,clusterBData)
      priorityQueue.enqueue((clusterA,clusterB,distance))
    }
  }

  /*
  * deducedLabels- map of cluster index to deduced cluster-label
  * */
  def applyLabels(finalClusters: mutable.HashMap[Int, mutable.Set[Int]], clusters: mutable.HashMap[Int, Array[Float]], labelsMap: mutable.HashMap[Int, String]) = {

    findLabels(finalClusters,labelsMap)
  }

  /*
  * clusterRep - clusterrepresentative
  * incorporatedPoints - points incorporated in the cluster represented by clusterRep
  * Assigns each final cluster a name by choosing the most frequently occurring class label of the examples in the
    cluster.
  * */
  def findLabels(finalClusters: mutable.HashMap[Int, mutable.Set[Int]], labelsMap: mutable.HashMap[Int, String])={

    val deducedLabels = mutable.HashMap[Int,String]()
    for (cluster <- finalClusters){
      val clusterRep = cluster._1
      val incorporatedPoints = cluster._2
      val labelsCount = mutable.HashMap[String,Int]().withDefaultValue(0)
      labelsCount(labelsMap(clusterRep)) += 1
      for (point <- incorporatedPoints){
        labelsCount(labelsMap(point)) += 1
      }
      val label = findMaxCountedLabel(labelsCount)
      deducedLabels.put(clusterRep,label)
    }
    deducedLabels
  }

  def findMaxCountedLabel(labelsCount: mutable.Map[String, Int]) = {
    var maxCountedLabel = ""
    var maxCount = 0
    for ((k,v)<- labelsCount){
      if (v>maxCount) {
        maxCountedLabel = k
        maxCount = v
      }
    }
    maxCountedLabel
  }

  def handleOutput(finalClusters: mutable.HashMap[Int, mutable.Set[Int]], labels: mutable.HashMap[Int, String], clusters: mutable.HashMap[Int, Array[Float]], labelsMap: mutable.HashMap[Int, String],fileName:String) = {
    val file = new File(fileName)
    val pw = new PrintWriter(file)
    for(cluster <- finalClusters){
      val label = labels(cluster._1)
      pw.write("cluster:"+label+"\n")
      pw.write(clusters(cluster._1).deep.mkString("[",", ",", '")+label+"']\n")
      for (point <- cluster._2){
        pw.write(clusters(point).deep.mkString("[",", ",", '")+label+"']\n")
      }
      pw.write("Number of points in this cluster:"+(cluster._2.size+1)+"\n")
      pw.write("\n")
    }
    pw.write("Number of points assigned to wrong cluster:"+countClustersWithWrongLabels(finalClusters,labels,labelsMap))
    pw.close()
  }

  def countClustersWithWrongLabels(finalClusters: mutable.HashMap[Int, mutable.Set[Int]], labels: mutable.HashMap[Int, String], labelsMap: mutable.HashMap[Int, String]) = {
    var countOfWrongLabels = 0;
    for(cluster<-finalClusters){
      val clusterRep = cluster._1
      val incorporatedPoints = cluster._2
      val label = labels(clusterRep)
      if (labelsMap(clusterRep) != label) countOfWrongLabels += 1
      for (point <- incorporatedPoints){
        if (labelsMap(point) != label) countOfWrongLabels += 1
      }
    }
    countOfWrongLabels
  }

  /*
  * merges the outliers as single clusters
  * */
  def mergeOutliers(clusters: mutable.HashMap[Int, mutable.Set[Int]], outliers: mutable.HashMap[Int, Array[Float]]) = {
    for(outlier <- outliers){
      if (!clusters.contains(outlier._1)) clusters.put(outlier._1,mutable.Set[Int]())
    }
  }

  def writeDendro(strings: ListBuffer[String]) = {
    val fileName = s"dendro_${150 - strings.size}.txt"
    val file = new File(fileName)
    val pw = new PrintWriter(file)
    pw.write(strings.mkString("\n"))
    pw.close()
  }
}
