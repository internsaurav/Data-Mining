package frequentItemsets

import scala.collection.mutable
import scala.collection.mutable.{HashMap, HashSet, Map}

object APriori {
  def runApriori( baskets:Array[Iterable[Int]], support:Int): Unit = {
    var (itemsIndex,itemCountsArray) = runPhase1(baskets)
    val (newItemIndex,numFreqSingletons) = runPhaseBeforePhase2(itemCountsArray,support)
    runPhase2(baskets,itemsIndex,newItemIndex,numFreqSingletons,support)


  }

  /*
  * runPhase1 is the first pass
  * In the first pass, we create two tables. The first table, if necessary, translates
    item names into integers from 1 to n, as described in Section 6.2.2. The other
    table is an array of counts; the ith array element counts the occurrences of the
    item numbered i. Initially, the counts for all the items are 0.
    As we read baskets, we look at each item in the basket and translate its
    name into an integer. Next, we use that integer to index into the array of
    counts, and we add 1 to the integer found there.
  * */

  def runPhase1(baskets: Array[Iterable[Int]]):(mutable.HashMap[Int,Int],Array[Int]) = {
    var itemCounts = new HashMap[Int,Int]().withDefaultValue(0) //itemCounts is the variable that stores the number of times each item has been rated.
    for (basket <- baskets){
      for (item <- basket){
        itemCounts(item) += 1;
      }
    }
    if (itemCounts contains(0)) itemCounts -= 0 //removes the zero value
    var itemsIndex = indexItems(itemCounts)
    itemCounts(0) = 0 //set count of item0 to 0
    val itemCountsArray = prepareCountsArray(itemCounts,itemsIndex)
    (itemsIndex,itemCountsArray)
  }


  /*
  * This translates
    itemIds into integers from 1 to n, as described in Section 6.2.2.
    form is [itemId -> someIndex]
  * */
  private def indexItems(itemCounts: Map[Int, Int]): mutable.HashMap[Int,Int]  = {
    var index=1
    var itemsIndex = new mutable.HashMap[Int,Int]()
    for ( (item,count) <- itemCounts){
      //index the items
      itemsIndex(item)=index
      index +=1
    }
    itemsIndex(0) = 0 //dummy index 0 to capture movieId 0. This is required for later stages
    itemsIndex
  }

  /*The methods returns an array of counts;
    the ith array element counts the occurrences of the item numbered i.
  * */
  private def prepareCountsArray(itemCounts: mutable.Map[Int, Int], itemIndex:mutable.HashMap[Int,Int]):Array[Int]={
    val countsArray = new Array[Int](itemIndex.size)
    for ((item,index) <- itemIndex){
      countsArray(index)=itemCounts(item)
    }
    countsArray
  }

  /*
  * runPhaseBeforePhase2
  * After the first pass, we examine the counts of the items to determine which of
    them are frequent as singletons.For the second pass of A-Priori, we create a new numbering from 1 to m for
    just the frequent items. This table is an array indexed 1 to n, and the entry
    for i is either 0, if item i is not frequent, or a unique integer in the range 1 to
    m if item i is frequent. We shall refer to this table as the frequent-items table.
  * */
  def runPhaseBeforePhase2(itemCountsArray:Array[Int], support:Int):(Array[Int],Int) = {
    val newItemIndex = new Array[Int](itemCountsArray.length)
    var newIndex = 1 //since 0 means not frequent
    for (i <- 0 until newItemIndex.length){
      if (itemCountsArray(i) >= support){
        newItemIndex(i) = newIndex
        newIndex += 1
      }
    }
    (newItemIndex,newIndex-1)
  }


  /*
    * During the second pass, we count all the pairs that consist of two frequent
      items. Recall from Section 6.2.3 that a pair cannot be frequent unless both its
      members are frequent. Thus, we miss no frequent pairs. The space required on
      the second pass is 2m^2 bytes, rather than 2n^2 bytes, if we use the triangular-
      matrix method for counting. Notice that the renumbering of just the frequent
      items is necessary if we are to use a triangular matrix of the right size. The
      complete set of main-memory structures used in the first and second passes is
      shown in Fig. 6.3.
      Also notice that the benefit of eliminating infrequent items is amplified; if
      only half the items are frequent we need one quarter of the space to count.
      Likewise, if we use the triples method, we need to count only those pairs of two
      frequent items that occur in at least one basket.
      The mechanics of the second pass are as follows.
      1. For each basket, look in the frequent-items table to see which of its items
      are frequent.
      2. In a double loop, generate all pairs of frequent items in that basket.
      3. For each such pair, add one to its count in the data structure used to
      store counts.
      Finally, at the end of the second pass, examine the structure of counts to
      determine which pairs are frequent.
      *freqItemsInBasket - contains the indices of the items not the ids
    * */
  def runPhase2(baskets:Array[Iterable[Int]], itemIndex:mutable.HashMap[Int,Int], newItemIndex: Array[Int], numFreqSingletons:Int,support:Int) = {
    var itemPairCountArray = new Array[Int](numFreqSingletons*numFreqSingletons/2)
    var indexItems = itemIndex.map(_.swap) //k,v reversed
    for (basket <- baskets){
      var freqItemsInBasket = new mutable.HashSet[Int]()
      for (item <- basket){
        val itemindex:Int = itemIndex(item)
        if (newItemIndex(itemindex) !=0 ) freqItemsInBasket += newItemIndex(itemindex)// just need to save their temporary indices
      }
      if (freqItemsInBasket.size > 1) itemPairCountArray = incrementValuesInTriangularMatrix(freqItemsInBasket,itemPairCountArray,itemIndex,indexItems,newItemIndex,numFreqSingletons)
    }

    var frequentPairs = findFrequentPairsFromMatrix(itemPairCountArray,support,newItemIndex,indexItems,numFreqSingletons)
    println(frequentPairs.mkString("\n"))
  }

  /*
  * The Triangular-Matrix Method
    Even after coding items as integers, we still have the problem that we must
    count a pair {i, j} in only one place. For example, we could order the pair so
    that i < j, and only use the entry a[i, j] in a two-dimensional array a. That
    strategy would make half the array useless. A more space-efficient way is to
    use a one-dimensional triangular array. We store in a[k] the count for the pair
    {i, j}, with 1 ≤ i < j ≤ n, where
    k= (i-1)(n-i/2) + j-i
    The result of this layout is that the pairs are stored in lexicographic order, that
    is first {1, 2}, {1, 3}, . . . , {1, n}, then {2, 3}, {2, 4}, . . . , {2, n}, and so on, down
    to {n − 2, n − 1}, {n − 2, n}, and finally {n − 1, n}.
    *but all of them wont be filled.
  * */
  private def incrementValuesInTriangularMatrix(freqItemsInBasket: mutable.HashSet[Int], itemPairCountArray: Array[Int], itemIndex: mutable.HashMap[Int, Int],indexItems:mutable.HashMap[Int, Int] , newItemIndex: Array[Int],numFreqSingletons:Int): Array[Int] = {
    var triangularMatrix = itemPairCountArray
    val n = numFreqSingletons
    val itr = freqItemsInBasket.subsets(2)
//    println("basket Recieved===>" + movieSetFromNewIndices(freqItemsInBasket,newItemIndex,indexItems).mkString(",") )
    while (itr.hasNext){
      val pair = itr.next().toArray
      var i,j = 0
      if (pair(0)<pair(1)){
        i=pair(0)
        j=pair(1)
      }
      else{
        i=pair(1)
        j=pair(0)
      }
//      println("pair considered is (" + movieIdFromNewIndices(i,newItemIndex,indexItems) + ","+ movieIdFromNewIndices(j,newItemIndex,indexItems)+")" )
val k = ((i-1)*(n-i.toFloat/2)+(j-i)).toInt
      triangularMatrix(k) += 1
    }
    triangularMatrix
  }

  /*
  * finds frequent items pairs
  */
  private def findFrequentPairsFromMatrix(itemPairCountArray: Array[Int],support:Int, newItemIndex: Array[Int], indexItems: mutable.HashMap[Int, Int],n:Int): mutable.SortedSet[(Int,Int)] ={
    var frequentPairs = mutable.SortedSet[(Int,Int)]()
    for (a <- 0 until newItemIndex.length-1){
      if (newItemIndex(a) !=0){
        for (b <- a+1 until newItemIndex.length){
          if (newItemIndex(b) !=0){
            val i = newItemIndex(a)
            val j = newItemIndex(b)
            val k = ((i-1)*(n-i.toFloat/2)+(j-i)).toInt
            if (itemPairCountArray(k) >= support){
              val item1 = indexItems(a)
              val item2 = indexItems(b)
              if (item1 < item2) frequentPairs += ((item1,item2)) else frequentPairs += ((item2,item1))
            }
          }
        }
      }
    }
  frequentPairs
  }

  private def findFrequentPairsNSupportFromMatrix(itemPairCountArray: Array[Int],support:Int, newItemIndex: Array[Int], indexItems: mutable.HashMap[Int, Int],n:Int): mutable.SortedSet[(Int,Int,Int)] ={
    var frequentPairs = mutable.SortedSet[(Int,Int,Int)]()
    for (a <- 0 until newItemIndex.length-1){
      if (newItemIndex(a) !=0){
        for (b <- a+1 until newItemIndex.length){
          if (newItemIndex(b) !=0){
            val i = newItemIndex(a)
            val j = newItemIndex(b)
            val k = ((i-1)*(n-i.toFloat/2)+(j-i)).toInt
            if (itemPairCountArray(k) >= support){
              val item1 = indexItems(a)
              val item2 = indexItems(b)
              if (item1 < item2) frequentPairs += ((item1,item2,itemPairCountArray(k))) else frequentPairs += ((item2,item1,itemPairCountArray(k)))
            }
          }
        }
      }
    }
    frequentPairs
  }

  /*
  * helper function to find the actual movie Ids from new ItemIndex
  * */
  private def movieSetFromNewIndices(freqItemsInBasket: mutable.HashSet[Int], newItemIndex: Array[Int], indexItems: mutable.HashMap[Int, Int]): mutable.SortedSet[Int]={
    var movieSet = mutable.SortedSet[Int]()
    for (i <- 0 until newItemIndex.length){
      if (newItemIndex(i)!=0 && freqItemsInBasket.contains(newItemIndex(i))) movieSet += indexItems(i)
    }
    movieSet
  }

  private def movieIdFromNewIndices(newIndex:Int, newItemIndex: Array[Int], indexItems: mutable.HashMap[Int, Int]): Int={
    var i=0
    for (j <- 0 until newItemIndex.length){
      if (newItemIndex(j) == newIndex) return indexItems(j)
    }
    indexItems(i)
  }

  private def checkIfUnique(newItemIndex: Array[Int], n:Int)={
    println("Num freq s " + n)
    var temp = new mutable.HashMap[Int,mutable.Set[(Int,Int)]]()
    var temp2 = new mutable.HashSet[Int]()
    for (a <- 0 until newItemIndex.length-1){
      if (newItemIndex(a) !=0){
        for (b <- a+1 until newItemIndex.length){
          if (newItemIndex(b) !=0){
            val i = newItemIndex(a)
            val j = newItemIndex(b)

            var k = (((i-1)*(n-(i.toFloat/2)))+(j-i)).toInt
            println("Considering " + i + "," + j + " K==> " + k)
            if (temp2.contains(k)) println("Duplicate") else temp2 += k
//            if (temp.contains(k)) temp(k) += ((i,j)) else temp += ((k,mutable.Set{(i,j)}))
          }
        }
      }
    }
//    print(temp.mkString("\n"))
  }

}
