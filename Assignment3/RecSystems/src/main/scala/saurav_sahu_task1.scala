package recSystems

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
    val (trainingDataKV,testingDataKV) = extractTrainingData(sc,ratingsFilePath,testDataPath)
    val ratingsRDD = sc.parallelize(trainingDataKV)
    val model = buildRecModel(ratingsRDD)
    val testingDataRDD = sc.parallelize(testingDataKV)
    val predictions = predictFromModesl(model,testingDataRDD)
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
    val testingDataTuples = sc.textFile(testDataPath).map(generateTestindDataKV).collect()
    var testingDataHashSet = new mutable.HashSet[(Int,Int)]()
    for (x <- testingDataTuples){
      testingDataHashSet += x
    }
    val testingDataBroadcastVar = sc.broadcast(testingDataHashSet)
    val trainingDataKV = sc.textFile(ratingsFilePath).mapPartitions(data => findTrainingAndData(data,testingDataBroadcastVar)).collect()
    testingDataBroadcastVar.destroy()
    (trainingDataKV,testingDataTuples)
  }

  /*
  * generates the userId,movieId key value pars from testing dataset
  * */
  def generateTestindDataKV(line:String):(Int,Int)={
    val lineSpit = line.split(",")
    if (lineSpit(0) != "userId") (lineSpit(0).toInt,lineSpit(1).toInt) else (0,0)
  }

  /*
  * it checks if the current data is in the testing set, if not emits the data
  * */
  def findTrainingAndData(data: Iterator[String], testingDataBroadcastVar: Broadcast[mutable.HashSet[(Int, Int)]]): Iterator[Rating] = {
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

  def buildRecModel(ratings:RDD[Rating]):MatrixFactorizationModel={
    val rank = 5
    val numIterations = 10
    val lambda = 0.01
    new ALS().setRank(rank).setIterations(numIterations).setLambda(lambda).run(ratings)
  }

  def predictFromModesl(model: MatrixFactorizationModel, testingDataRDD: RDD[(Int, Int)]) = {
    val x = model.predict(testingDataRDD).collect()
    println(x.mkString("\n"))
  }
}
