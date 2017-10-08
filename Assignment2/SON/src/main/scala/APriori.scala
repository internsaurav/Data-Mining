package frequentItemsets

import scala.collection.immutable.{HashSet, SortedSet}
import scala.collection.{immutable, mutable}
import scala.collection.mutable.{HashMap, Map}
import scala.util.control.Breaks.{break, breakable}

object APriori {

  /*
  * This function runs Apriori till no more frequent items are found
  * It runs phase 1 and 2 pHase 2 manually. This is because Phase implements upper triangular matrix
  * Further phases run in a loop and use triples method to store the data.
  * */
  def runApriori( baskets:Array[Iterable[Int]], support:Int): Unit = {
    var frequentItemsSets = new mutable.HashMap[Int,HashSet[Set[Int]]]()
    var (itemsIndex,itemCountsArray) = runPhase1(baskets)
    val (newItemIndex,numFreqSingletons) = runPhaseBeforePhase2(itemCountsArray,support)
    var frequentCombosFoundInCurrentPhase = (numFreqSingletons != 0) //this variable is the flag used to stop the loop when no more frequent items are found
    var frequentPairs = runPhase2(baskets,itemsIndex,newItemIndex,numFreqSingletons,support)
//    frequentItemsSets = addResultsofPhase1(newItemIndex,frequentItemsSets)
//    println(frequentPairs.mkString("  "))
    frequentItemsSets += ((2,frequentPairs))
//    println(frequentItemsSets(2).mkString("\n"))
    frequentCombosFoundInCurrentPhase = (frequentPairs.size != 0)
    var phase = 3 //initialises phase 3
    while (frequentCombosFoundInCurrentPhase){
      var thisPhaseFreqItemsets:HashSet[Set[Int]] = runPhaseN(baskets,phase,frequentItemsSets,support)
      frequentCombosFoundInCurrentPhase = (thisPhaseFreqItemsets.size != 0)
      if (frequentCombosFoundInCurrentPhase) frequentItemsSets(phase)=thisPhaseFreqItemsets
      phase +=1
    }
    val temp2 = frequentItemsSets(3)
    println(temp2.to[List].mkString("\n"))
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
  def runPhase2(baskets:Array[Iterable[Int]], itemIndex:mutable.HashMap[Int,Int], newItemIndex: Array[Int], numFreqSingletons:Int,support:Int):HashSet[immutable.Set[Int]] = {
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
    new HashSet[Set[Int]]++ frequentPairs
  }

   /*
        *runs nphase of Apriori
        We can follow this pattern as far as we wish. The set C 3 of candidate
        triples is constructed (implicitly) as the set of triples, any two of which is a
        pair in L 2 . Our assumption about the sparsity of frequent itemsets, outlined
        in Section 6.2.4 implies that there will not be too many frequent pairs, so they
        can be listed in a main-memory table. Likewise, there will not be too many
        candidate triples, so these can all be counted by a generalization of the triples
        method. That is, while triples are used to count pairs, we would use quadruples,
        consisting of the three item codes and the associated count, when we want to
        count triples. Similarly, we can count sets of size k using tuples with k + 1
        components, the last of which is the count, and the first k of which are the item
        codes, in sorted order.
        To find L 3 we make a third pass through the basket file. For each basket,
        we need only look at those items that are in L 1 . From these items, we can
        examine each pair and determine whether or not that pair is in L 2 . Any item
        of the basket that does not appear in at least two frequent pairs, both of which
        consist of items in the basket, cannot be part of a frequent triple that the
        basket contains. Thus, we have a fairly limited search for triples that are both
        contained in the basket and are candidates in C 3 . Any such triples found have
        1 added to their count.
        for higher orders, I am creating a gatekeeper singleton array from last freuent itemset. this will save time i guess
        we dont need the new indices in triples approach
        * */
  def runPhaseN(baskets: Array[Iterable[Int]], phase: Int, frequentItemsSets: mutable.HashMap[Int, HashSet[Set[Int]]],support:Int): HashSet[Set[Int]] = {
    val lastFreqItemSet:HashSet[Set[Int]] = frequentItemsSets(phase-1)
    val tempFreqSingletonsInLastItemSet:HashSet[Int]=makeSingletons(lastFreqItemSet)
    var candidatePairs = new mutable.HashMap[Set[Int],Int]()
    for (basket <- baskets){
      var freqItemsInBasket = new mutable.HashSet[Int]()
      for (item <- basket){
        if (tempFreqSingletonsInLastItemSet.contains(item)) freqItemsInBasket += item
      }
      if (freqItemsInBasket.size >= phase) candidatePairs = updateCandidatePair(candidatePairs,(new HashSet[Int]++freqItemsInBasket),lastFreqItemSet,phase)
    }
    findTrulyFrequent(candidatePairs,support)
  }

  def findTrulyFrequent(candidatePairs: mutable.HashMap[Set[Int], Int], support: Int):HashSet[Set[Int]] = {
    var temp = new mutable.HashSet[Set[Int]]()
    for ((candidate,count) <- candidatePairs){
      if (count >= support) temp += candidate
    }
//    val tempSorted = SortedSet[Set[Int]]()++temp
    new HashSet[Set[Int]]++temp
  }

  def updateCandidatePair(candidatePairs: mutable.HashMap[Set[Int], Int], freqItemsInBasket: HashSet[Int], lastFreqItemSet:HashSet[Set[Int]],phase:Int): _root_.scala.collection.mutable.HashMap[immutable.Set[Int], Int] = {

    var tempCP = candidatePairs
    val itr1 = freqItemsInBasket.subsets(phase)
    while (itr1.hasNext){
      val combo = itr1.next()
      if (tempCP.contains(combo)){
        tempCP(combo) += 1
      } else {
        var candidate = true
        val itr2 = combo.subsets(phase-1)
        breakable(
          while(itr2.hasNext){
            val constituent = itr2.next()
            if (!lastFreqItemSet.contains(constituent)){
              candidate = false
              break()
            }
          })
        if (candidate){
          val temp = SortedSet[Int]()++combo
          tempCP(temp)=1
        }
      }
    }
    tempCP
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
val k = ((i-1)*(n-i.toFloat/2)+(j-i)).toInt
      triangularMatrix(k) += 1
    }
    triangularMatrix
  }

  /*
  * finds frequent items pairs
  */
  private def findFrequentPairsFromMatrix(itemPairCountArray: Array[Int],support:Int, newItemIndex: Array[Int], indexItems: mutable.HashMap[Int, Int],n:Int): mutable.HashSet[immutable.Set[Int]] ={
    var frequentPairs = mutable.HashSet[immutable.Set[Int]]()
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
              if (item1 < item2) frequentPairs += immutable.Set(item1,item2) else frequentPairs += immutable.Set(item2,item1)
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

  def makeSingletons(lastFreqItemSet: HashSet[Set[Int]]):HashSet[Int]={
    var temp = new mutable.HashSet[Int]()
    for(itemSet <- lastFreqItemSet){
      for (item <- itemSet){
        temp += item
      }
    }
    new HashSet[Int]()++temp
  }

//  private def addResultsofPhase1(newItemIndex: Array[Int], frequentItemsSets: mutable.HashMap[Int, Any]):mutable.HashMap[Int,Any]={
//    var frequentItemsSets = new mutable.HashMap[Int,Any]()
//
//  }
}
