import java.util.Calendar
import java.lang.Math._

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object SparkScala3 {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Hello World")
      .getOrCreate()

    val sc = sparkSession.sparkContext

    val rdd1 = sc.parallelize(List(1, 2, 3, 4, 5, 6))

    println(rdd1.collect().mkString(","))

    println(rdd1.reduce(_ + _))
    val sum = rdd1.reduce((x, y) => max(x, y))
    println(sum)

    val wordsRDD1 = sc.parallelize(
      List(
        "Good",
        "Morning",
        "Hello",
        "World",
        "Hi",
        "Hello",
        "India",
        "Karnataka",
        "India"
      )
    )

    wordsRDD1.persist(StorageLevel.MEMORY_ONLY)
    //wordsRDD1.cache()
    val start = Calendar.getInstance().getTimeInMillis
    println(wordsRDD1.count())
    println(wordsRDD1.countByValue())
    println(wordsRDD1.first())
    println(wordsRDD1.take(3).mkString(",")) // First 3 elements of RDD
    println(wordsRDD1.top(4).mkString(",")) //Top 4 from descending order of elements
    println(wordsRDD1.takeOrdered(4).mkString(",")) //Top 4 from ascending order of elements

    println("Duration : " + (Calendar.getInstance().getTimeInMillis - start))
    //wordsRDD1.unpersist()

    val rdd7 = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8), 4)

    val result = rdd7.fold(10)((x, y) => max(x, y))
    println("fold example with max:")
    println("Result: " + result)

    println("Num of partitions: " + rdd7.partitions.length)
    val result2 = rdd7.fold(10)((x, y) => (x + y))
    println(result2)

    val result4 = rdd7.aggregate(6)((x, y) => (x + y), (x, y) => (x + y))
    println("result4: " + result4)
    val result3 = rdd7.aggregate(0, 0)(
      (acc, value) => (acc._1 + value, acc._2 + 1),
      (x, y) => (x._1 + y._1, x._2 + y._2)
    )

    println("result3: " + result3)
    println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    val seqOp = (s: (Int, Int), v: Int) =>
      if (v % 2 == 0) (s._1 + v, s._2) else (s._1, s._2 + v)
    val combOp = (s1: (Int, Int), s2: (Int, Int)) =>
      (s1._1 + s2._1, s1._2 + s2._2)

    //val result5 = rdd7.aggregate(0,0) ( (s,v) => if ( v % 2 == 0 ) (s._1 + v, s._2) else (s._1,s._2 + v) , (s1,s2) => (s1._1 + s2._1, s1._2 + s2._2) )
    val result5 = rdd7.aggregate(0, 0)(seqOp, combOp)
    println("Sum of Odd Even is = " + result5._1 + " , " + result5._2)
    println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    println("avg: " + (result3._1.toFloat / result3._2.toFloat))

    //val wordRDD = sc.textFile("D:\\student.txt")
    //wordRDD.flatMap(x => x.split(" ")).filter( x => x.length() > 3).collect.foreach(println)

    println("Enter string:")
    val str = scala.io.StdIn.readLine()

  }

}
