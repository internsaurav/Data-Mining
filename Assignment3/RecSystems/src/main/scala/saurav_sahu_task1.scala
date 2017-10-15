package recSystems

import org.apache.spark.{SparkContext,SparkConf}
import collection.mutable
/*
*/
object saurav_sahu_task1 {
  def main(args: Array[String])={

    val ratingsFilePath = args(0)
    val testDataPath = args(1)
    val sc = makeSparkContext()
    extractTrainingData(sc,ratingsFilePath,testDataPath)
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
  }
  def generateTestindDataKV(line:String):(Int,Int)={
    val lineSpit = line.split(",")
    if (lineSpit(0) != "userId") (lineSpit(0).toInt,lineSpit(1).toInt) else (0,0)
  }
}
