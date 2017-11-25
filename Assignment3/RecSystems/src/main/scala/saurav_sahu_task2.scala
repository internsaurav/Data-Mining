package recSystems

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.recommendation.Rating
import recSystems.saurav_sahu_task1.{extractTrainingData, makeSparkContext,printAccuracyInfo,handleOutput}

import scala.collection.mutable

object saurav_sahu_task2 {

  def main(args: Array[String])={
    val startTime = System.currentTimeMillis()
    val ratingsFilePath = args(0)
    val testDataPath = args(1)
    val moviesDataPath = args(2)
    val sc = makeSparkContext()
    val itemBased = true
    val hp = new Hyperparameters(70,5,0.4)
    val (trainingDataKV,testingDataKV,testingGroundDataKV) = extractTrainingData(sc,ratingsFilePath,testDataPath)
    val trainingDataAsTuples = representAsTuples(sc,trainingDataKV)
    val (usersIndex,itemsIndex) = reIndexUsersAndItems(trainingDataAsTuples.keySet)
    val userItemRatingsArray = prepareRatingsArray(trainingDataAsTuples,usersIndex,itemsIndex,itemBased)
    var predictions = CFinMapReduce(itemBasedCF,sc,testingDataKV,userItemRatingsArray,usersIndex,itemsIndex,hp)
    val (rareItems,ratinglessCombos) = findRareItems(predictions)
    val similarItemsForRareItems = findSimilarItemsFromAbsoluteGenre(sc,rareItems,moviesDataPath)
    val rarestOfRareItems = findRarestOfRare(similarItemsForRareItems)
    val similarItemsForRarestOfRareItems = findSimilarItemsFromGenre(sc,rarestOfRareItems,moviesDataPath,hp)
    val hpPhase2 = new Hyperparameters(10,50)
    val ratingsForRareItems = findRatingsForRarestOfRare(sc,ratinglessCombos,similarItemsForRareItems,similarItemsForRarestOfRareItems,userItemRatingsArray,usersIndex,itemsIndex,hpPhase2)
    predictions = incorporateNewRatings(predictions,ratinglessCombos,ratingsForRareItems)
    handleOutput("saurav_sahu_result_task2.txt",predictions,testingGroundDataKV)
    var noRatingsCount = 0
    for((k,v) <- predictions){
      if(v==0.0) noRatingsCount += 1
    }
    println(s"No ratings found in $noRatingsCount cases")
    println(s"The total execution time taken is ${(System.currentTimeMillis() - startTime)/(1000)} sec.")
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
  def prepareRatingsArray(trainingDataAsTuples:scala.collection.Map[(Int,Int),Double], usersIndex: mutable.HashMap[Int, Int], itemsIndex: mutable.HashMap[Int, Int], isItemBased:Boolean) = {
    val ratingsArray = Array.ofDim[Double](itemsIndex.size+1,usersIndex.size+1) // item 0 and user 0 are dummy ones
    for (((user,item),rating) <- trainingDataAsTuples){
      ratingsArray(itemsIndex(item))(usersIndex(user)) = rating
    }
    if (isItemBased) ratingsArray else ratingsArray.transpose
  }

  /*
     *There are 4 possible cases
     * User present, Item present -> we do collaborative filtering for these cases only,
     * User absent, Item present  ->  this is a cold start problem. So we take average over all ratings.
     * User present, Item absent ->
     * User and Item both absent -> this and the above case are filtered. The ratings for such cases are done with genres.
     */
  def itemBasedCF(testingDataKV: Iterator[(Int, Int)], userItemRatingsArray:Broadcast[Array[Array[Double]]], usersIndex:Broadcast[mutable.HashMap[Int, Int]], itemsIndex:Broadcast[ mutable.HashMap[Int, Int]],hpBV:Broadcast[Hyperparameters]) = {
    val similarItemDictionary = mutable.HashMap[Int,IndexedSeq[(Int,Float)]]()
    val predictedRatings = mutable.HashMap[(Int,Int),Double]()
    val predictFromGenreFlag = 0.0
    while (testingDataKV.hasNext){
      val userItemSample = testingDataKV.next()
      val user = userItemSample._1
      val item = userItemSample._2
      if (!usersIndex.value.contains(user) && itemsIndex.value.contains(item)) {
        predictedRatings((user,item)) = averageOverAllRatings(itemsIndex.value(item),userItemRatingsArray.value)
      }
      else if (!itemsIndex.value.contains(item)){
        predictedRatings((user,item)) = saurav_sahu_comp.NewItemFlag
      } else {
        val itemIndex = itemsIndex.value(item)
        var similarItemsMap = IndexedSeq[(Int,Float)]()
        if (!similarItemDictionary.contains(itemIndex)){
          similarItemsMap = findSimilarItemsIndex(itemIndex,userItemRatingsArray.value,hpBV.value).toIndexedSeq.sortWith(_._2 > _._2)
//          similarItemsMap = amplify(similarItemsMap,0.6)
          similarItemDictionary(itemIndex) = similarItemsMap
        } else similarItemsMap = similarItemDictionary(itemIndex)
        val userIndex = usersIndex.value(user)
        val itemRating = findRatingFromSimilarItems(userIndex,similarItemsMap,userItemRatingsArray.value,hpBV.value)
        predictedRatings((user,item)) = itemRating
      }
    }
    predictedRatings.toIterator
  }

  //finds the similar items using Pearson Similarity
  def findSimilarItemsIndex(row: Int, userItemRatingsArray: Array[Array[Double]], hp:Hyperparameters) = {
    val similarItemsMap = new mutable.HashMap[Int,Float]()
    for (i <- 1 until userItemRatingsArray.length ){
      val (pearsonCorrCoefft,numItemsRated,sumOfAllRatings) = pearsonCorrelationCoefficient(userItemRatingsArray(row),userItemRatingsArray(i),hp)
      if (pearsonCorrCoefft != 0.0) similarItemsMap(i) = pearsonCorrCoefft.toFloat
    }
    similarItemsMap.remove(row)
    similarItemsMap
  }

  def pearsonCorrelationCoefficient(i: Array[Double], j: Array[Double],hp:Hyperparameters) = {
    val numUsers = i.length
    val (iAvg,jAvg,numCoratingUsers,numRatingsByJ,sumOfRatingsByJ) = findAverageRating(i,j)
    var pcc = 0.0
    val minimumNumberOfCoratingUsers = hp.minimumCoratingusers
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
    (pcc,numRatingsByJ,sumOfRatingsByJ)
  }

  def findAverageRating(i: Array[Double], j: Array[Double]) = {
    val numUsers = i.length
    var count = 0
    var sum1,sum2 =0.0
    var sumOfRatingsByj=0.0
    var numRatingsByj=0
    for (k <- 1 until numUsers){
      val rki = i(k)
      val rkj = j(k)
      if (rkj != 0.0){
        sumOfRatingsByj += rkj
        numRatingsByj += 1
      }
      if (rki != 0.toDouble && rkj != 0.toDouble){
        sum1 += rki
        sum2 += rkj
        count += 1
      }
    }
    if (count !=0)(sum1/count,sum2/count,count,numRatingsByj,sumOfRatingsByj) else (0.0,0.0,count,numRatingsByj,sumOfRatingsByj)
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
  def findRatingFromSimilarItems(column: Int, similarItemsSeq: IndexedSeq[(Int, Float)], userItemRatingsArray: Array[Array[Double]],hp:Hyperparameters) = {
    val neighbourhoodSize = hp.neighbourhoodSize
    var numerator,denominator =0.0
    var defaultReturnValue = saurav_sahu_comp.NewItemFlag
    var count,i = 0
    while (count < neighbourhoodSize && i < similarItemsSeq.length){
      val similarItemWithPCC = similarItemsSeq(i)
      val pcc = similarItemWithPCC._2
      val row = similarItemWithPCC._1
      val ratingByUserForItem = userItemRatingsArray(row)(column)
      if (ratingByUserForItem != 0.0){
        count += 1
        numerator += ratingByUserForItem*pcc
        denominator += math.abs(pcc)
      }
      i += 1
    }
    if (denominator !=0.0) numerator/denominator else defaultReturnValue
  }

  def printUserCol(user: Int, similarItemsSeq: IndexedSeq[(Int, Float)], userItemRatingsArray: Array[Array[Double]]) = {
    for ((x,y) <- similarItemsSeq){
      print(userItemRatingsArray(x)(user)+",")
    }
    println()
  }

  def CFinMapReduce(basisOfCF:(Iterator[(Int, Int)], Broadcast[Array[Array[Double]]], Broadcast[mutable.HashMap[Int, Int]], Broadcast[ mutable.HashMap[Int, Int]],Broadcast[Hyperparameters]) => Iterator[((Int,Int),Double)], sc: SparkContext, testingDataKV: Array[(Int, Int)], userItemRatingsArray: Array[Array[Double]], usersIndex: mutable.HashMap[Int, Int], itemsIndex: mutable.HashMap[Int, Int],hp:Hyperparameters) = {
    val userItemRatingsArrayBV = sc.broadcast(userItemRatingsArray)
    val usersIndexBV = sc.broadcast(usersIndex)
    val itemsIndexBV = sc.broadcast(itemsIndex)
    val hpBV = sc.broadcast(hp)
    val temp = sc.parallelize(testingDataKV).mapPartitions(data => basisOfCF(data,userItemRatingsArrayBV,usersIndexBV,itemsIndexBV,hpBV)).collectAsMap()
    userItemRatingsArrayBV.destroy()
    usersIndexBV.destroy()
    itemsIndexBV.destroy()
    hpBV.destroy()
    temp
  }

  def findRareItems(predictions: collection.Map[(Int, Int), Double]) = {
    var setOfFriendLessItems = Set[Int]()
    var ratinglessCombos = Set[(Int,Int)]()
    for ((k,v)<-predictions){
      if (v == saurav_sahu_comp.NewItemFlag) {
        setOfFriendLessItems += k._2
        ratinglessCombos += k
      }
    }
    (setOfFriendLessItems,ratinglessCombos)
  }

  def averageOverAllRatings(item: Int, value: Array[Array[Double]]): Double = {
    val thisItem = value(item)
    val defaultRatingForUnseenItems = saurav_sahu_comp.NewItemFlag
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


  /*
  * uses exact genre set as provided in the movies.csv to find similar movies
  * itemGenreKV - Key value pair of (item,genreSet)
  * genreMap - A map with key as GenreSEt and values as te set of movies having this genreSet
  * itemGenreMap - map version of itemGenreKV
  * */
  def findSimilarItemsFromAbsoluteGenre(sc:SparkContext, itemsWithoutNeighbours: Set[Int], moviesDataPath: String) = {
    val itemGenreKV = sc.textFile(moviesDataPath).mapPartitionsWithIndex(findMovieAbsoluteGenres).collect()
    //    println(userGenreKV.mkString("\n"))
    val genreMap = sc.parallelize(itemGenreKV).mapPartitions(createGenreMap).reduceByKey((a,b) => a.union(b)).collectAsMap()
    val itemGenreMap = sc.parallelize(itemGenreKV).collectAsMap()
    val itemGenreMapBV = sc.broadcast(itemGenreMap)
    val genreMapBV  = sc.broadcast(genreMap)
    val temp = sc.parallelize(itemsWithoutNeighbours.toSeq).mapPartitions(x => findSimilarAbsoluteGenres(x,itemGenreMapBV,genreMapBV)).collectAsMap()
    itemGenreMapBV.destroy()
    genreMapBV.destroy()
    temp
  }

  //this returns the list of similar Items and their jaccard Similarities
  def findSimilarItemsFromGenre(sc: SparkContext, rareItems: Set[Int], moviesDataPath: String, hp:Hyperparameters) = {
    val itemGenreKV = sc.textFile(moviesDataPath).mapPartitionsWithIndex(findMovieGenres).collect()
    val itemGenreIndicesKV = assignIndexToGenres(itemGenreKV)
    val itemGenreIndicesBV = sc.broadcast(itemGenreIndicesKV)
    val hpBV = sc.broadcast(hp)
    val x = sc.parallelize(rareItems.toSeq).mapPartitions(x=>findSimilarItemsWithJaccard(x,itemGenreIndicesBV,hpBV)).collectAsMap()
    itemGenreIndicesBV.destroy()
    hpBV.destroy()
    x
  }

  def findSimilarItemsWithJaccard(x: Iterator[Int], reindexedUserGenreBV: Broadcast[mutable.HashMap[Int, Set[Int]]], hpBV:Broadcast[Hyperparameters]) = {
    val itemNeighboursMap = new mutable.HashMap[Int,Set[(Int,Float)]]()
    val minimumJaccardSimilarity = hpBV.value.jaccardSim
    while (x.hasNext){
      val rareItem =   x.next()
      val rareItemGenreSet = reindexedUserGenreBV.value(rareItem)
      var jaccSimSet = Set[(Int,Float)]()
      for ((item, genreSet) <- reindexedUserGenreBV.value){
        val jaccardSim = jaccardSimilarity(rareItemGenreSet,genreSet)
        if (jaccardSim >= minimumJaccardSimilarity) jaccSimSet += ((item,jaccardSim))
      }
      itemNeighboursMap(rareItem) = jaccSimSet
    }
    itemNeighboursMap.toIterator
  }

  //jaccard Similarity
  def jaccardSimilarity(x: Set[Int], y: Set[Int]): Float = {
    val xIntersectY = x.intersect(y).size.toFloat
    val xUnionY =  x.union(y).size.toFloat
    xIntersectY/xUnionY
  }

  /*
   *
   * THis creates an index of genres. Basically maps each genre to an index. this leads to faster comparison as comapred to string comparisons.
   *
   */
  def assignIndexToGenres(itemGenreKV: Array[(Int, Set[String])]) = {
    val genreIndexMap = mutable.HashMap[String,Int]()
    val reindexedItemGenreKV = mutable.HashMap[Int,Set[Int]]()
    var index = 0
    for ((item,genreSet) <- itemGenreKV){
      for (genre <- genreSet){
        if (!genreIndexMap.contains(genre)){
          genreIndexMap(genre) = index
          index += 1
        }
      }
    }
    for ((item,genreSet) <- itemGenreKV){
      reindexedItemGenreKV(item) = genreSet.map(x => genreIndexMap(x))
    }
    reindexedItemGenreKV
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

  //generates movie and genre tuples. genre is considered as a set of genres
  def findMovieGenres(index:Int,data:Iterator[String])={
    if (index == 0) data.next()
    var movieGenreTuples = Set[(Int,Set[String])]()
    while (data.hasNext){
      val movie =  data.next()
      val movieSplit = movie.split(",")
      movieGenreTuples += ((movieSplit(0).toInt,movieSplit.last.split('|').toSet))
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

  //this returns the list of similar Items and their absolute genres
  def findSimilarAbsoluteGenres(x:Iterator[Int], itemGenreMapBV: Broadcast[collection.Map[Int,String]], genreMapBV:Broadcast[scala.collection.Map[String,Set[Int]]]) = {
    val temp = mutable.HashMap[Int,Set[Int]]()
    while (x.hasNext){
      val thisX = x.next()
      val xGenreSet = itemGenreMapBV.value(thisX)
      val similarSet = genreMapBV.value(xGenreSet) - thisX
      temp(thisX)=similarSet
    }
    temp.toIterator
  }

  //finds ratings for those items who could not find friends in the initial phase
  def findRatingsForRarestOfRare(sc: SparkContext, ratinglessCombos: Set[(Int,Int)], similarItemsForRareItems: collection.Map[Int, Set[Int]], similarItemsForRarestItems:collection.Map[Int,Set[(Int,Float)]], userItemRatingsArray:Array[Array[Double]], usersIndex:mutable.HashMap[Int, Int], itemsIndex: mutable.HashMap[Int, Int],hpPhase2:Hyperparameters) = {
    val userItemRatingsArrayBV = sc.broadcast(userItemRatingsArray)
    val usersIndexBV = sc.broadcast(usersIndex)
    val itemsIndexBV = sc.broadcast(itemsIndex)
    val hpPhase2BV = sc.broadcast(hpPhase2)
    val rareTtems = removeRarestFromRare(similarItemsForRareItems,similarItemsForRarestItems)
    val rareItemsNeighboursBV = sc.broadcast(rareTtems)
    val rarestItemsNeighboursBV = sc.broadcast(similarItemsForRarestItems)
    val ratingsForRareItems =sc.parallelize(ratinglessCombos.toSeq).mapPartitions(data => findSimilarRatingsForItemsWithoutFriendsinNode(data,rareItemsNeighboursBV,rarestItemsNeighboursBV,userItemRatingsArrayBV,usersIndexBV,itemsIndexBV,hpPhase2BV)).collectAsMap()
    usersIndexBV.destroy()
    itemsIndexBV.destroy()
    rareItemsNeighboursBV.destroy()
    rarestItemsNeighboursBV.destroy()
    hpPhase2BV.destroy()
    ratingsForRareItems
  }

  //removes the rarest of rare items from rare items
  def removeRarestFromRare(items: collection.Map[Int, Set[Int]], rarestItems: collection.Map[Int, Set[(Int, Float)]]): _root_.scala.collection.Map[Int, _root_.scala.Predef.Set[Int]] = {
    var temp = items
    for (x <- rarestItems.keySet){
      temp -= x
    }
    temp
  }

  //finds SImilar Ratings for items that could not find any friends in the first phase
  def findSimilarRatingsForItemsWithoutFriendsinNode(data: Iterator[(Int,Int)], rareItemsNeighboursBV:Broadcast[collection.Map[Int, Set[Int]]], rarestItemsNeighboursBV: Broadcast[collection.Map[Int,Set[(Int,Float)]]], userItemRatingsArrayBV: Broadcast[Array[Array[Double]]], usersIndexBV: Broadcast[mutable.HashMap[Int, Int]], itemsIndexBV: Broadcast[mutable.HashMap[Int, Int]],hpPhase2BV:Broadcast[Hyperparameters])= {
    val predictedRatings = mutable.HashMap[(Int,Int),Double]()
    val coldStartProblemFlag = saurav_sahu_comp.NewItemFlag
    val defaultRating = 3.0
    while (data.hasNext){
      val userItem = data.next()
      val userId = usersIndexBV.value(userItem._1)
      val indexOfNeighbours = findIndexOfNeighbours(userItem._2,rareItemsNeighboursBV.value,rarestItemsNeighboursBV.value,itemsIndexBV.value)
      var itemRating = findRatingFromSimilarItems(userId,indexOfNeighbours,userItemRatingsArrayBV.value,hpPhase2BV.value)
      if (itemRating == coldStartProblemFlag){
        if (itemsIndexBV.value.contains(userItem._2))itemRating = averageOverAllRatings(itemsIndexBV.value(userItem._2),userItemRatingsArrayBV.value) else itemRating = coldStartProblemFlag
//        itemRating = defaultRating
      }
      predictedRatings((userItem._1,userItem._2)) = itemRating
    }
    predictedRatings.toIterator
  }

  /*
   *FInds neighbours onl from items list not users list
   */
  def findIndexOfNeighbours(item:Int,rareItemsNeighboursBV: collection.Map[Int, Set[Int]], rarestItemsNeighboursBV: collection.Map[Int, Set[(Int, Float)]], itemsIndexBV: mutable.HashMap[Int, Int]) = {
    var indexOfNeighbours = IndexedSeq[(Int,Float)]()
    if (rareItemsNeighboursBV.contains(item)){
      val neighbours = rareItemsNeighboursBV(item).map(x => (x,1.toFloat))
      indexOfNeighbours = removeRarestAndFindIndex(neighbours,itemsIndexBV)
    } else if (rarestItemsNeighboursBV.contains(item)){
      indexOfNeighbours = removeRarestAndFindIndex(rarestItemsNeighboursBV(item),itemsIndexBV).sortWith(_._2 > _._2)
    }
    indexOfNeighbours
  }

  //removes those rarest of rare items which are themselves not rated in the training data
  def removeRarestAndFindIndex(friends: Set[(Int,Float)], itemsIndex: mutable.HashMap[Int, Int])={
    var temp = Set[(Int,Float)]()
    for (x <- friends){
      if (itemsIndex.contains(x._1)){
        temp += ((itemsIndex(x._1),x._2))
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

  def findRarestOfRare(similarItemsForRareItems: collection.Map[Int, Set[Int]]) = {
    var rarestOfRare = Set[Int]()
    val minimumNeighbourhoodSize = 50
    for ((k,v) <- similarItemsForRareItems){
      if (v.size < minimumNeighbourhoodSize) rarestOfRare += k
    }
    rarestOfRare
  }

  def findRarestOfRare2(similarItemsForRareItems: collection.Map[Int, Set[Int]], minNeighbourhoodSize:Int) = {
    var rarestOfRare = Set[Int]()
    val minimumNeighbourhoodSize = minNeighbourhoodSize
    for ((k,v) <- similarItemsForRareItems){
      if (v.size < minimumNeighbourhoodSize) rarestOfRare += k
    }
    rarestOfRare
  }

  def amplify(similarItemsMap: IndexedSeq[(Int, (Float,Int,Double))], factor:Double) = {
    similarItemsMap.map(x=>(x._1,(x._2._1*Math.pow(Math.abs(x._2._1),factor).toFloat,x._2._2,x._2._3)))
  }
}
