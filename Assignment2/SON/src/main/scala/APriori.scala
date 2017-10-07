package frequentItemsets

import scala.collection.mutable.{HashMap, HashSet,Map}

object APriori {
  def runApriori( baskets:Array[Iterable[Int]], support:Int): Unit = {
    var (frequentSingletons,moviesIndex) = runPhase1(baskets,support)
    var frequentMoviesRenumbered = prepareFreuentItemsTable(frequentSingletons,moviesIndex)

  }
  def runPhase1(baskets: Array[Iterable[Int]], support:Int): (HashSet[Int],Array[Int]) = {
    var movieCounts = new HashMap[Int,Int]().withDefaultValue(0) //movieCounts is the variable that stores the number of times each movie has been rated.
    for (basket <- baskets){
      for (movie <- basket){
        movieCounts(movie) += 1;
      }
    }
    if (movieCounts contains(0)) movieCounts -= 0 //removes the zero value
    var (frequentItems,moviesIndex) = indexMoviesAndFindFrequent(movieCounts,support)
    (frequentItems,moviesIndex)
  }

  def indexMoviesAndFindFrequent(movieCounts: Map[Int, Int], support: Int): (HashSet[Int], Array[Int])  = {
    var frequentItems = HashSet[Int]()
    var index=0
    var moviesIndex = new Array[Int](movieCounts.size)
    for ( (movie,count) <- movieCounts){
      //index the movies
      moviesIndex(index)=movie
      index +=1
      if (count >= support) frequentItems.add(movie)
    }
    (frequentItems,moviesIndex)
  }

  def prepareFreuentItemsTable(frequentSingletons: HashSet[Int], moviesIndex: Array[Int]):Array[Int] = {
    var frequentMoviesRenumbered = new Array[Int](moviesIndex.length)
    var renumberIndex =0
    for (i <- 0 until frequentMoviesRenumbered.length){
      val movieId = moviesIndex(i)
      if (frequentSingletons.contains(movieId)){
        frequentMoviesRenumbered(i)=renumberIndex
        renumberIndex += 1
      }
      else frequentMoviesRenumbered(i) = 0
    }
    frequentMoviesRenumbered
  }

}
