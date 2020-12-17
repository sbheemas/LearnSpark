import org.apache.spark.sql.SparkSession

object MapPartitionsDemo {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Hello World")
      .config("spark.default.parallelism", "5")
      .getOrCreate()
    val sc = sparkSession.sparkContext
    println("Default parallelism")
    println(sc.defaultParallelism)

    val data = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8), 3)
    println()

    data
      .mapPartitionsWithIndex((index, words) => {
        for (word <- words) yield (word, index)
      })
      .collect
      .foreach(println)
    println(data.map(sum).collect.mkString(","))
    println("Sum of numbers in each partition:")

    println(data.mapPartitions(sumPartition).collect.mkString(","))

    println("Highest in each partition")
    println(data.mapPartitions(highestInPartition).collect.mkString(","))

    println("Sum of keys and values in each partition: ")
    val rdd1 = sc.parallelize(
      List((1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8)),
      4
    )

    rdd1
      .mapPartitionsWithIndex((index, words) => {
        for (word <- words) yield (word, index)
      })
      .collect
      .foreach(println)
    rdd1.mapPartitions(sumPartitionPairs).collect.foreach(println)

  }

  def sum(number: Int): Int = {
    var sum = 1;
    return sum + number
  }

  def sumPartition(numbers: Iterator[Int]): Iterator[Int] = {
    var sum = 0
    while (numbers.hasNext) {
      sum = sum + numbers.next()
    }
    return Iterator(sum)
  }

  def highestInPartition(numbers: Iterator[Int]): Iterator[Int] = {
    var max = 0
    while (numbers.hasNext) {
      val currVal = numbers.next()
      if (currVal > max) {
        max = currVal
      }
    }
    return Iterator(max)
  }

  def sumPartitionPairs(numbers: Iterator[(Int, Int)]): Iterator[(Int, Int)] = {
    var sumKey = 0
    var sumVal = 0
    while (numbers.hasNext) {
      val curVal = numbers.next()
      sumKey = sumKey + curVal._1
      sumVal = sumVal + curVal._2
    }
    val retval = (sumKey, sumVal)
    return Iterator(retval)
  }
}
