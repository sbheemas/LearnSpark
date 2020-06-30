import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession

object PartAssignment {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local").appName("Hello World").getOrCreate()
    val sc = sparkSession.sparkContext

    val rdd1 = sc.textFile("data\\Student2.txt")
    val rdd2 = rdd1.map(splitMethod)
    val rdd3 = rdd2.partitionBy(new HashPartitioner(3))
    rdd3.mapPartitionsWithIndex((index, words)=>{for (word <- words) yield(word,index)}).foreach(println)
    println("Lowest:")
    rdd3.mapPartitions(x => lowestValue(x)).foreach(println)
    println("Highest")
    rdd3.mapPartitions(x => highestValue(x)).foreach(println)
    println("Average:")
    rdd3.mapPartitions(x => avgValue(x)).foreach(println)
  }

  def splitMethod(x:String) :(String,Int) = {
    val splitWord = x.split(":")
    (splitWord(0) , splitWord(1).toInt)
    }

  def lowestValue(elements : Iterator[(String,Int)]): Iterator[Int] = {
    var min=Integer.MAX_VALUE
    for( y <- elements) {
      if (y._2 < min) {
        min = y._2
      }
    }
    Iterator(min)
  }

  def highestValue(elements : Iterator[(String,Int)]): Iterator[Int] = {
    var max=Integer.MIN_VALUE
    for( y <- elements) {
      if (y._2 > max) {
        max = y._2
      }
    }
    Iterator(max)
  }

  def avgValue( elements : Iterator[(String,Int)]) : Iterator[Double] = {
    var sum=0D
    var count = 0D
    var avg=0D
    for(y <- elements) {
      sum += y._2
      count +=1
    }
    if(count > 0) {
      avg = sum / count
    }
    Iterator(avg)
  }
}
