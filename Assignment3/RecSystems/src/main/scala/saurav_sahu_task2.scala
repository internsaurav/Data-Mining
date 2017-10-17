package recSystems

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.recommendation.Rating
import recSystems.saurav_sahu_task1.{extractTrainingData, makeSparkContext,printAccuracyInfo,writeSortedOutput}

import scala.collection.mutable

object saurav_sahu_task2 {

  def main(args: Array[String])={
    val ratingsFilePath = args(0)
    val testDataPath = args(1)
    val moviesDataPath = args(2)
    val sc = makeSparkContext()
    val (trainingDataKV,testingDataKV,testingGroundDataKV) = extractTrainingData(sc,ratingsFilePath,testDataPath)
//    println(testingDataKV.mkString(","))
    //map ratings to tuple form
    val trainingDataAsTuples = representAsTuples(sc,trainingDataKV)
    val (usersIndex,itemsIndex) = reIndexUsersAndItems(trainingDataAsTuples.keySet)
    val userItemRatingsArray = prepareRatingsArray(trainingDataAsTuples,usersIndex,itemsIndex)
    val predictions = itemBasedCFinMapReduce(sc,testingDataKV,userItemRatingsArray,usersIndex,itemsIndex)
//    println(predictions.mkString("\n"))
    printAccuracyInfo(predictions,testingGroundDataKV)
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

  def itemBasedCF(testingDataKV: Iterator[(Int, Int)], userItemRatingsArray:Broadcast[Array[Array[Double]]], usersIndex:Broadcast[mutable.HashMap[Int, Int]], itemsIndex:Broadcast[ mutable.HashMap[Int, Int]]) = {
    val similarItemDictionary = mutable.HashMap[Int,IndexedSeq[(Int,Float)]]()
    val predictedRatings = mutable.HashMap[(Int,Int),Double]()
    val defaultRating = 3.2
    while (testingDataKV.hasNext){
      val userItemSample = testingDataKV.next()
      val user = userItemSample._1
      val item = userItemSample._2
      if (!itemsIndex.value.contains(item) || !usersIndex.value.contains(user)) {
        predictedRatings((user,item)) = defaultRating
      } else {
        val itemIndex = itemsIndex.value(item)
        var similarItemsMap = IndexedSeq[(Int,Float)]()
        if (!similarItemDictionary.contains(itemIndex)){
          similarItemsMap = findSimilarItemsIndex(itemIndex,userItemRatingsArray.value).toIndexedSeq.sortWith(_._2 > _._2)
          similarItemDictionary(itemIndex) = similarItemsMap
        } else similarItemsMap = similarItemDictionary(itemIndex)
        val userIndex = usersIndex.value(user)
        val itemRating = findRatingFromSimilarItems(userIndex,similarItemsMap,userItemRatingsArray.value)
        predictedRatings((user,item)) = itemRating
      }
    }
    predictedRatings.toIterator
  }

  //finds the similar items using Pearson Similarity
  def findSimilarItemsIndex(item: Int,userItemRatingsArray: Array[Array[Double]]) = {
    val similarItemsMap = new mutable.HashMap[Int,Float]()
    for (i <- 1 until userItemRatingsArray.length ){
      val pearsonCorrCoefft = pearsonCorrelationCoefficient(userItemRatingsArray(item),userItemRatingsArray(i))
      if (pearsonCorrCoefft != 0.0) similarItemsMap(i) = pearsonCorrCoefft.toFloat
    }
    similarItemsMap.remove(item)
    similarItemsMap
  }

  def pearsonCorrelationCoefficient(i: Array[Double], j: Array[Double]) = {
    val numUsers = i.length
    val (iAvg,jAvg,numCoratingUsers) = findAverageRating(i,j)
    var pcc = 0.0
    val minimumNumberOfCoratingUsers = 50
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

  private def checkIfArrayHasNoNonZeroRatings(userItemRatingsArray: Array[Array[Double]]) = {
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

  def findRatingFromSimilarItems(user: Int, similarItemsSeq: IndexedSeq[(Int, Float)],userItemRatingsArray: Array[Array[Double]]) = {
    val neighbourhoodSize = 50
//    println(neighbourhoodSize)
    var numerator,denominator =0.0
    val defaultRating = 3.4
    var count,i = 0
    while (count < neighbourhoodSize && i < similarItemsSeq.length){
      val similarItemWithPCC = similarItemsSeq(i)
      val pcc = similarItemWithPCC._2
      val item = similarItemWithPCC._1
      val ratingByUserForItem = userItemRatingsArray(item)(user)
      if (ratingByUserForItem != 0.0){
        count += 1
        numerator += ratingByUserForItem*pcc
        denominator += math.abs(pcc)
      }
      i += 1
    }
    if (denominator !=0.0) numerator/denominator else defaultRating
  }

  def itemBasedCFinMapReduce(sc: SparkContext, testingDataKV: Array[(Int, Int)], userItemRatingsArray: Array[Array[Double]], usersIndex: mutable.HashMap[Int, Int], itemsIndex: mutable.HashMap[Int, Int]) = {
    val userItemRatingsArrayBV = sc.broadcast(userItemRatingsArray)
    val usersIndexBV = sc.broadcast(usersIndex)
    val itemsIndexBV = sc.broadcast(itemsIndex)
    val temp = sc.parallelize(testingDataKV).mapPartitions(data => itemBasedCF(data,userItemRatingsArrayBV,usersIndexBV,itemsIndexBV)).collectAsMap()
    userItemRatingsArrayBV.destroy()
    usersIndexBV.destroy()
    itemsIndexBV.destroy()
    temp
  }
}
