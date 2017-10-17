package recSystems

import java.io.{File, PrintWriter}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD

import collection.mutable

/*
*/
object saurav_sahu_task1 {

  def main(args: Array[String]):Unit={

    val ratingsFilePath = args(0)
    val testDataPath = args(1)
    val sc = makeSparkContext()
    val (trainingDataKV,testingDataKV,testingGroundDataKV) = extractTrainingData(sc,ratingsFilePath,testDataPath)
    val ratingsRDD = sc.parallelize(trainingDataKV)
    val model = buildRecModel(ratingsRDD)
    val testingDataRDD = sc.parallelize(testingDataKV)
    val predictions = predictFromModesl(model,testingDataRDD)
    printAccuracyInfo(predictions,testingGroundDataKV)
    writeSortedOutput(predictions)
    sc.stop()
  }

  def makeSparkContext():SparkContext={
    val appName = "recSystem"
    val master = "local[*]" // uses as many cores as present in local machine
    val conf = new SparkConf().setMaster(master).setAppName(appName)
    new SparkContext(conf) //spark context is the interface with cluster
  }

  /*
    Specifically,	you	should	first	extract	training	data	from
    *the	ratings.csv	file	downloaded	from	Movie	Lens	using	the	testing	data.
    */
  def extractTrainingData(sc:SparkContext,ratingsFilePath:String,testDataPath:String)={
    val testingDataTuples = sc.textFile(testDataPath).mapPartitionsWithIndex((ind,itr) =>generateTestindDataKV(ind,itr)).collect()
    var testingDataHashSet = new mutable.HashSet[(Int,Int)]()
    for (x <- testingDataTuples){
      testingDataHashSet += x
    }
    val testingDataBroadcastVar = sc.broadcast(testingDataHashSet)
    val trainingDataKV = sc.textFile(ratingsFilePath).mapPartitions(data => findTrainingData(data,testingDataBroadcastVar)).collect()
    var testingGroundDataKV = sc.textFile(ratingsFilePath).mapPartitions(data => findTestingData(data,testingDataBroadcastVar)).collectAsMap()
    testingDataBroadcastVar.destroy()
    (trainingDataKV,testingDataTuples,testingGroundDataKV)
  }

  /*
  * generates the userId,movieId key value pars from testing dataset
  * */
  def generateTestindDataKV(ind:Int,data:Iterator[String]):Iterator[(Int,Int)]={
    var seq = mutable.HashSet[(Int,Int)]()
    if (ind == 0) data.next()
    while (data.hasNext){
      val line = data.next()
      val lineSpit = line.split(",")
      seq += ((lineSpit(0).toInt,lineSpit(1).toInt))
    }
    seq.toIterator
  }

  /*
  * it checks if the current data is in the testing set, if not emits the data
  * */
  def findTrainingData(data: Iterator[String], testingDataBroadcastVar: Broadcast[mutable.HashSet[(Int, Int)]]): Iterator[Rating] = {
    var trainingDataSet = new mutable.HashSet[Rating]()
    for (line <- data){
      val lineSplit = line.split(",")
      if (lineSplit(0) != "userId"){
        val userId = lineSplit(0).toInt
        val movieId = lineSplit(1).toInt
        val rating = lineSplit(2).toDouble
        if (!testingDataBroadcastVar.value.contains((userId,movieId))) trainingDataSet += Rating(userId,movieId,rating)
      }
    }
    trainingDataSet.toIterator
  }

  def findTestingData(data: Iterator[String], testingDataBroadcastVar: Broadcast[mutable.HashSet[(Int, Int)]]): Iterator[((Int,Int),Double)] = {
    var trainingDataSet = new mutable.HashMap[(Int,Int),Double]()
    for (line <- data){
      val lineSplit = line.split(",")
      if (lineSplit(0) != "userId"){
        val userId = lineSplit(0).toInt
        val movieId = lineSplit(1).toInt
        val rating = lineSplit(2).toDouble
        if (testingDataBroadcastVar.value.contains((userId,movieId))) trainingDataSet += (((userId,movieId),rating))
      }
    }
    trainingDataSet.toIterator
  }

  def buildRecModel(ratings:RDD[Rating]):MatrixFactorizationModel={
    val rank = 3
    val numIterations = 10
    val lambda = 0.01
    new ALS().setRank(rank).setIterations(numIterations).setLambda(lambda).run(ratings)
  }

  def predictFromModesl(model: MatrixFactorizationModel, testingDataRDD: RDD[(Int, Int)]) = {
    val x = model.predict(testingDataRDD).map{ case Rating(user, product, rate) => ((user, product), rate)}.collectAsMap()
    x
  }


  def printAccuracyInfo(predictions: collection.Map[(Int, Int), Double], testingGroundDataKV: collection.Map[(Int, Int), Double]) = {
//    var accuracyInfo = mutable.HashMap[String,Int](">=0 and <1"->0, ">=1 and <2"->0, ">=2 and <3" ->0, ">=3 and <4" ->0, ">=4" -> 0)
    var accuracyInfo = mutable.HashMap[Int,Int](0->0, 1->0, 2 ->0, 3 ->0, 4 -> 0)
    var sumOfSquaresOfErrors = 0.0
    val defaultRating = 3.0
    val numElements = testingGroundDataKV.size
    for((k,v) <- testingGroundDataKV){
      var err =0.0
      if (predictions.contains(k)) err = math.abs(predictions(k)-v) else err = math.abs(defaultRating-v)
      sumOfSquaresOfErrors += err*err
      if (err >= 4) accuracyInfo(4) += 1 else accuracyInfo(math.floor(err).toInt) +=1
    }
    println(">=0 and <1: " + accuracyInfo(0))
    println(">=1 and <2: " + accuracyInfo(1))
    println(">=2 and <3: " + accuracyInfo(2))
    println(">=3 and <4: " + accuracyInfo(3))
    println(">=4: " + accuracyInfo(4))
    println("RMSE = " + math.sqrt(sumOfSquaresOfErrors/numElements))
  }

  def writeSortedOutput(predictions: collection.Map[(Int, Int), Double]) = {

    var outputFileName = "saurav_sahu_result_task1.txt"
    //    println(frequentItemSets)
    val file = new File(outputFileName)
    val pw = new PrintWriter(file)
    var sortedKeySet = mutable.SortedSet[(Int,Int)]()
    for ((k,v) <- predictions){
      sortedKeySet += k
    }
    for(x <- sortedKeySet){
      pw.write( x._1 + "," + x._2 + ","+ predictions(x) + "\n")
    }
    pw.close()
  }
}
