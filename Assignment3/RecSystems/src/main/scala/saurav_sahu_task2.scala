package recSystems

import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.Rating
import recSystems.saurav_sahu_task1.{extractTrainingData, makeSparkContext}

import scala.collection.mutable

object saurav_sahu_task2 {

  def main(args: Array[String])={
    val ratingsFilePath = args(0)
    val testDataPath = args(1)
    val sc = makeSparkContext()
    val (trainingDataKV,testingDataKV,testingGroundDataKV) = extractTrainingData(sc,ratingsFilePath,testDataPath)
    //map ratings to tuple form
    val trainingDataAsTuples = representAsTuples(sc,trainingDataKV)
    val (usersIndex,itemsIndex) = reIndexUsersAndItems(trainingDataAsTuples.keySet)
//    println(s"Num users are : ${usersIndex.size} ")
//    println(s"Num items are : ${itemsIndex.size} ")
    val userItemRatingsArray = prepareRatingsArray(trainingDataAsTuples,usersIndex,itemsIndex)
//    checkIfArrayHasNoNonZeroRatings(userItemRatingsArray)
//    for (x <- userItemRatingsArray){
//      println(x.mkString(","))
//    }
//    println(userItemRatingsArray(1).deep.mkString("  "))
    val predictions = itemBasedCF(sc,testingDataKV,userItemRatingsArray,usersIndex,itemsIndex)
    sc.stop()
  }

  def representAsTuples(sc:SparkContext, trainingDataKV: Array[Rating]) = {
    sc.parallelize(trainingDataKV).map{case Rating(user, product, rate) => ((user, product), rate)}.collectAsMap()
  }

  //finds the number of users and items in the set. this is importatn because i cant assume that the users will be continuous.
  def reIndexUsersAndItems(keySet: collection.Set[(Int, Int)]) = {
    val usersIndex = new mutable.HashMap[Int,Int]()
    val itemsIndex = new mutable.HashMap[Int,Int]()
    var i,j = 1
    for ((user,item) <- keySet ){
      if( !usersIndex.contains(user)){
        usersIndex += ((user,i))
        i +=1
      }
      if( !itemsIndex.contains(item)){
        itemsIndex += ((item,j))
        j +=1
      }
    }
    (usersIndex,itemsIndex)
  }

  //prepares the ratings array. the rows are items and columns are users
  def prepareRatingsArray(trainingDataAsTuples:scala.collection.Map[(Int,Int),Double], usersIndex: mutable.HashMap[Int, Int], itemsIndex: mutable.HashMap[Int, Int]) = {
    val ratingsArray = Array.ofDim[Double](itemsIndex.size+1,usersIndex.size+1) // item 0 and user 0 are dummy ones
    for (((user,item),rating) <- trainingDataAsTuples){
      ratingsArray(itemsIndex(item))(usersIndex(user)) = rating
    }
    ratingsArray
  }

  def itemBasedCF(sc: SparkContext, testingDataKV: Array[(Int, Int)], userItemRatingsArray: Array[Array[Double]], usersIndex: mutable.HashMap[Int, Int], itemsIndex: mutable.HashMap[Int, Int]) = {
    val userItemSample = testingDataKV(60)//0 is 0,0
    val user = userItemSample._1
    val item = userItemSample._2
    val itemIndex = itemsIndex(item)
    val similarItemsMap = findSimilarItemsIndex(itemIndex,userItemRatingsArray)
    println(similarItemsMap.mkString("\n"))
//    val p = userItemRatingsArray(820)
//    val q= userItemRatingsArray(3331)
//    for (i<- 1 until p.length){
//      if (p(i) != 0.0 && q(i)!=0.0){
//        println( p(i))
//        println(q(i))
//        println("===========")
//      }
//    }
  }

  //finds the similar items using Pearson Similarity
  def findSimilarItemsIndex(item: Int,userItemRatingsArray: Array[Array[Double]]) = {
    val similarItemsMap = new mutable.HashMap[Int,Float]()
    for (i <- 1 until userItemRatingsArray.length ){
      val pearsonCorrCoefft = pearsonCorrelationCoefficient(userItemRatingsArray(item),userItemRatingsArray(i))
      if (pearsonCorrCoefft != 0.0) similarItemsMap(i) = pearsonCorrCoefft.toFloat
    }
    similarItemsMap
  }

//  def pearsonCorrelationCoefficient(i: Int, j: Int,userItemRatingsArray: Array[Array[Double]]) = {
//    val numUsers = userItemRatingsArray(0).length
//    val (iAvg,jAvg,noCoratingUsers) = findAverageRating(i,j,userItemRatingsArray)
//    var pcc = 0.toDouble
//    if (!noCoratingUsers) {
//      var numerator = 0.toDouble
//      var denominator1 = 0.toDouble
//      var denominator2 = 0.toDouble
//      for (k <- 1 until userItemRatingsArray(0).length) {
//        //corated items
//        val rki = userItemRatingsArray(i)(k)
//        val rkj = userItemRatingsArray(j)(k)
//        if (rki != 0.toDouble && rkj != 0.toDouble) {
//          numerator += (rki - iAvg) * (rkj - jAvg)
//          denominator1 += (rki - iAvg) * (rki - iAvg)
//          denominator2 += (rkj - jAvg) * (rkj - jAvg)
//        }
//      }
//      if (denominator1 != 0.toDouble && denominator2 != 0.toDouble){
//        pcc = numerator/(math.sqrt(denominator1)*math.sqrt(denominator2))
//      }
//    }
//    pcc
//  }

  def pearsonCorrelationCoefficient(i: Array[Double], j: Array[Double]) = {
    val numUsers = i.length
    val (iAvg,jAvg,numCoratingUsers) = findAverageRating(i,j)
    var pcc = 0.0
    val minimumNumberOfCoratingUsers = 5
    if (numCoratingUsers > minimumNumberOfCoratingUsers) {
      var numerator = 0.0
      var denominator1 = 0.0
      var denominator2 = 0.0
      for (k <- 1 until i.length) {
        //corated items
        val rki = i(k)
        val rkj = j(k)
        if (rki != 0.0 && rkj != 0.0) {
          numerator += (rki - iAvg) * (rkj - jAvg)
          denominator1 += (rki - iAvg) * (rki - iAvg)
          denominator2 += (rkj - jAvg) * (rkj - jAvg)
        }
      }
      if (denominator1 != 0.0 && denominator2 != 0.0){
        pcc = numerator/(math.sqrt(denominator1)*math.sqrt(denominator2))
      }
    }
    pcc
  }

  def findAverageRating(i: Array[Double], j: Array[Double]) = {
    val numUsers = i.length
    var count = 0
    var sum1,sum2 =0.0
    for (k <- 1 until numUsers){
      val rki = i(k)
      val rkj = j(k)
      if (rki != 0.toDouble && rkj != 0.toDouble){
        sum1 += rki
        sum2 += rkj
        count += 1
      }
    }
    if (count !=0)(sum1/count,sum2/count,count) else (0.0,0.0,count)
  }

  private def printColumn(col:Int,arr:Array[Array[Double]])={
    for (i <- 0 until arr.length){
      print(arr(i)(col) + ",")
    }
    println()
  }

  private  def checkIfArrayHasNoNonZeroRatings(userItemRatingsArray: Array[Array[Double]]) = {
    var flag = true
    for ( i <- 0 until userItemRatingsArray.length ){
      var x = userItemRatingsArray(i)
      for (j <- 0 until x.length){
        if (x(j)!= 0.toDouble) flag=false
      }
      if (flag) println("item "+i+" is blank")
      flag = true
    }
  }
}
