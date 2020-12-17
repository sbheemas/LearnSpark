import org.apache.spark
import org.apache.spark.HashPartitioner
import org.apache.spark.RangePartitioner
import org.apache.spark.sql.SparkSession

object PartitioningDemo {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Hello World")
      .getOrCreate()
    val sc = sparkSession.sparkContext

    val rdd1 = sc.textFile("data\\inputfile.txt")

    val rdd2 = rdd1
      .flatMap(x => x.split(" "))
      .map(x => (x, 1))
      .partitionBy(new HashPartitioner(3))
    println(rdd2.getNumPartitions)
    rdd2.keys.collect().foreach(println)
    rdd2.values.collect().foreach(println)
    val rdd9 = rdd2.map(x => x + "word")
    rdd9.collect().foreach(println)
    println(rdd9.getNumPartitions)

    val str = "sankir"
    println(str.hashCode())
    println("bangalore".hashCode())

    val rdd3 = sc.parallelize(
      List(
        (1, 1),
        (3, 2),
        (4, 3),
        (2, 1),
        (0, 2),
        (20, 3),
        (6, 1),
        (5, 2),
        (7, 1),
        (8, 3),
        (50, 1)
      ),
      2
    )

    println("Num of partitions: " + rdd3.getNumPartitions)

    val rdd4 = rdd3.partitionBy(new HashPartitioner(3))

    rdd4
      .mapPartitionsWithIndex(((i, words) => (for (w <- words) yield (w, i))))
      .collect
      .foreach(println)
    println("Num of partitions: " + rdd4.getNumPartitions)

    rdd4.persist()
    val rdd5 = rdd4.repartitionAndSortWithinPartitions(new HashPartitioner(4))
    rdd5.persist()
    println(rdd5.getNumPartitions)
    println("Hash-partitioned RDD:")
    rdd5
      .mapPartitionsWithIndex((index, words) => {
        for (word <- words) yield (word, index)
      })
      .collect()
      .foreach(println)
    println("Range-partitioned RDD:")

    val rdd12 = sc.parallelize(List((6, 2), (0, 1), (3, 3), (20, 1)))
    val rdd11 = rdd3.partitionBy(new RangePartitioner(4, rdd12, true))

    rdd11
      .mapPartitionsWithIndex((index, words) => {
        for (word <- words) yield (word, index)
      })
      .foreach(println)

    val rdd6 = rdd3.partitionBy(new RangePartitioner(4, rdd3, true))

    rdd6
      .mapPartitionsWithIndex((index, words) => {
        for (word <- words) yield (word, index)
      })
      .collect()
      .foreach(println)

    val rdd7 = sc.textFile("data\\student.txt")

    val rdd8 = rdd7
      .flatMap(x => x.split(" "))
      .map(x => (x, 1))
      .partitionBy(new HashPartitioner(3))
    rdd8.persist()
    rdd8
      .mapPartitions(words => { for (word <- words) yield (word, 1) })
      .collect()
      .foreach(println)

    println("mapPartitionsWithIndex.....")
    rdd8
      .mapPartitionsWithIndex((index, words) => {
        for (word <- words) yield (word, index)
      })
      .collect()
      .foreach(println)

    val rdd10 = sc.parallelize(List(("aaa", 10), ("bbb", 20), ("ccc", 30)))
    println(rdd10.getNumPartitions)

  }

}
