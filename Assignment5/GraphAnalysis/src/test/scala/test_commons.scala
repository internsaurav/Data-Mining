package GraphAnalysis
import org.scalatest.FunSuite
import GirvanNewman._
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{IntegerType, StructType,StringType}
import org.graphframes.GraphFrame

import scala.collection.mutable

class test_commons extends FunSuite{

  private def findIndexInTriangularMat(numUsers:Int, x:Int, y:Int):Int={
    var i = math.min(x,y)
    var j = math.max(x,y)
    var temp = ((i-1)*(numUsers-i.toFloat/2)+(j-i)).toInt
    println(s"found index for userIndex:${i},userIndex:${j} ====> ${temp}")
    temp
  }

  private def verifyTriangularmat(numUsers:Int): Unit ={
    val x = numUsers*numUsers/2
    println(s"As per existing implementation, size of array should be $x and indices range from 0 to ${x-1} ")
    println("Printing out the array indices used:")
    for (i <- 1 until numUsers){
      for (j<- i+1 to numUsers){
        val k = ((i-1)*(numUsers-i.toFloat/2)+(j-i)).toInt
        println(s"$i  $j  ==> $k ")
      }
    }
  }
}