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
    var predictions = itemBasedCFinMapReduce(sc,testingDataKV,userItemRatingsArray,usersIndex,itemsIndex)
    val (rareItems,ratinglessCombos) = findRareItems(predictions)
    val similarItemsForRareItems = findSimilarItemsFromAbsoluteGenre(sc,rareItems,moviesDataPath)
    val ratingsForRareItems = findRatingsForsimilarItemsWithoutNeighbours(sc,ratinglessCombos,similarItemsForRareItems,userItemRatingsArray,usersIndex,itemsIndex)
    predictions = incorporateNewRatings(predictions,ratinglessCombos,ratingsForRareItems)
    //    println(rareItems.size)
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
    val predictFromGenreFlag = 0.0
    while (testingDataKV.hasNext){
      val userItemSample = testingDataKV.next()
      val user = userItemSample._1
      val item = userItemSample._2
      if (!usersIndex.value.contains(user)) {
        predictedRatings((user,item)) = averageOverAllRatings(itemsIndex.value(item),userItemRatingsArray.value)
      }
      else if (!itemsIndex.value.contains(item)){
        predictedRatings((user,item)) = predictFromGenreFlag
      } else {
        val itemIndex = itemsIndex.value(item)
        var similarItemsMap = IndexedSeq[(Int,Float)]()
        if (!similarItemDictionary.contains(itemIndex)){
          similarItemsMap = findSimilarItemsIndex(itemIndex,userItemRatingsArray.value).toIndexedSeq.sortWith(_._2 > _._2)
          similarItemDictionary(itemIndex) = similarItemsMap
        } else similarItemsMap = similarItemDictionary(itemIndex)
        val userIndex = usersIndex.value(user)
        val itemRating = findRatingFromSimilarItems(userIndex,similarItemsMap,userItemRatingsArray.value,false)
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
//    if (similarItemsMap.size == 0) println("Map is empty")
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

 //flag useDefaultRating means whether to use a default rating or to check use hybrind methods. If hybrid Methods maul, then we use defaut
  def findRatingFromSimilarItems(user: Int, similarItemsSeq: IndexedSeq[(Int, Float)],userItemRatingsArray: Array[Array[Double]],useDefaultRating:Boolean) = {
    val neighbourhoodSize = 50
    //    println(neighbourhoodSize)
    var numerator,denominator =0.0
    val defaultRating = 3.0
    val predictFromGenreFlag = 0.0
    var defaultReturnValue = 0.0
    if (useDefaultRating) defaultReturnValue = defaultRating else defaultReturnValue = predictFromGenreFlag
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
    if (denominator !=0.0) numerator/denominator else defaultReturnValue
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

  def findRareItems(predictions: collection.Map[(Int, Int), Double]) = {
    var setOfFriendLessItems = Set[Int]()
    var ratinglessCombos = Set[(Int,Int)]()
    for ((k,v)<-predictions){
      if (v == 0.0) {
        setOfFriendLessItems += k._2
        ratinglessCombos += k
      }
    }
    (setOfFriendLessItems,ratinglessCombos)
  }

  def averageOverAllRatings(item: Int, value: Array[Array[Double]]): Double = {
    val thisItem = value(item)
    val defaultRatingForUnseenItems = 3.0
    var count = 0
    var sum =0.0
    for (k <- 1 until thisItem.length){
      val rki = thisItem(k)
      if (rki != 0.toDouble){
        sum += rki
        count += 1
      }
    }
    if (count !=0)(sum/count) else defaultRatingForUnseenItems
  }

  def findSimilarItemsFromAbsoluteGenre(sc:SparkContext, itemsWithoutNeighbours: Set[Int], moviesDataPath: String) = {
    val userGenreKV = sc.textFile(moviesDataPath).mapPartitionsWithIndex(findMovieAbsoluteGenres).collect()
    //    println(userGenreKV.mkString("\n"))
    val genreMap = sc.parallelize(userGenreKV).mapPartitions(createGenreMap).reduceByKey((a,b) => a.union(b)).collectAsMap()
    val userGenreMap = sc.parallelize(userGenreKV).collectAsMap()
    val userGenreMapBV = sc.broadcast(userGenreMap)
    val genreMapBV  = sc.broadcast(genreMap)
    val temp = sc.parallelize(itemsWithoutNeighbours.toSeq).mapPartitions(x => findSimilarAbsoluteGenres(x,userGenreMapBV,genreMapBV)).collectAsMap()
    userGenreMapBV.destroy()
    temp
  }

  //generates movie and genre tuples
  def findMovieAbsoluteGenres(index:Int, data:Iterator[String])={
    if (index == 0) data.next()
    var movieGenreTuples = Set[(Int,String)]()
    while (data.hasNext){
      val movie =  data.next()
      val movieSplit = movie.split(",")
      movieGenreTuples += ((movieSplit(0).toInt,movieSplit.last))
    }
    movieGenreTuples.toIterator
  }

  //creates a map of genre to movieIds. This is useless
  def createGenreMap(data:Iterator[(Int,String)])={
    var genreMovie = mutable.HashMap[String,Set[Int]]()
    while(data.hasNext){
      val movieGenre = data.next()
      if (genreMovie.contains(movieGenre._2)) genreMovie(movieGenre._2) += movieGenre._1 else genreMovie(movieGenre._2) = Set(movieGenre._1)
    }
    genreMovie.toIterator
  }

  //this returns the list of similar Items and their jaccard Similarities
  def findSimilarAbsoluteGenres(x:Iterator[Int], userGenreMapBV: Broadcast[collection.Map[Int,String]],genreMapBV:Broadcast[scala.collection.Map[String,Set[Int]]]) = {
    val temp = mutable.HashMap[Int,Set[Int]]()
    while (x.hasNext){
      val thisX = x.next()
      val xGenreSet = userGenreMapBV.value(thisX)
      val similarSet = genreMapBV.value(xGenreSet) - thisX
      temp(thisX)=similarSet
    }
    temp.toIterator
  }

  //finds ratings for those items who could not find friends in the initial phase
  def findRatingsForsimilarItemsWithoutNeighbours(sc: SparkContext, ratinglessCombos: Set[(Int,Int)] ,items: collection.Map[Int, Set[Int]],userItemRatingsArray:Array[Array[Double]], usersIndex:mutable.HashMap[Int, Int], itemsIndex: mutable.HashMap[Int, Int]) = {
    val userItemRatingsArrayBV = sc.broadcast(userItemRatingsArray)
    val usersIndexBV = sc.broadcast(usersIndex)
    val itemsIndexBV = sc.broadcast(itemsIndex)
    val itemsFriendsMaps = sc.broadcast(items)
    val ratings =sc.parallelize(ratinglessCombos.toSeq).mapPartitions(data => findSimilarRatingsForItemsWithoutFriendsinNode(data,itemsFriendsMaps,userItemRatingsArrayBV,usersIndexBV,itemsIndexBV)).collectAsMap()
    usersIndexBV.destroy()
    itemsIndexBV.destroy()
    itemsFriendsMaps.destroy()
    ratings
  }

  //finds SImilar Ratings for items that could not find any friends in the first phase
  def findSimilarRatingsForItemsWithoutFriendsinNode(data: Iterator[(Int,Int)],itemsFriendsMap:Broadcast[collection.Map[Int, Set[Int]]], userItemRatingsArrayBV: Broadcast[Array[Array[Double]]], usersIndexBV: Broadcast[mutable.HashMap[Int, Int]], itemsIndexBV: Broadcast[mutable.HashMap[Int, Int]])= {
    val predictedRatings = mutable.HashMap[(Int,Int),Double]()
    while (data.hasNext){
      val userItem = data.next()
      val userId = usersIndexBV.value(userItem._1)
      val friends = itemsFriendsMap.value(userItem._2)
      val indexOffriends = removeRarestAndIndex(friends,itemsIndexBV.value)
      //      println(indexOffriends)
      val itemRating = findRatingFromSimilarItems(userId,indexOffriends,userItemRatingsArrayBV.value,true)
      predictedRatings((userItem._1,userItem._2)) = itemRating
    }
    predictedRatings.toIterator
  }

  //removes those rarest of rare items which are themselves not rated in the training data
  def removeRarestAndIndex(friends: Set[Int], itemsIndex: mutable.HashMap[Int, Int])={
    var temp = Set[(Int,Float)]()
    for (x <- friends){
      if (itemsIndex.contains(x)){
        temp += ((itemsIndex(x),1.toFloat))
      }
    }
    temp.toIndexedSeq
  }

  def incorporateNewRatings(predictions: collection.Map[(Int, Int), Double], ratinglessCombos: Set[(Int, Int)], ratingsForRareItems: collection.Map[(Int, Int), Double]): _root_.scala.collection.Map[(Int, Int), Double] = {
    var temp = new mutable.HashMap[(Int,Int),Double]() ++ predictions
    for (x <- ratinglessCombos){
      val rating = ratingsForRareItems(x)
      temp(x) = rating
    }
    temp
  }

}
