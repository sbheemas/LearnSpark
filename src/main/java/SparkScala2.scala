import org.apache.spark.sql.SparkSession

object SparkScala2 {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().master("local").appName("Hello World").getOrCreate()
    val sc = sparkSession.sparkContext
    val rdd1 = sc.parallelize(List(1,2,3,4,5,6,9))
    val rdd2 = sc.parallelize(List(1,2,3,4,5,6,7,8))
    println(rdd1.union(rdd2).collect().mkString(","))
    println(rdd1.intersection(rdd2).collect().mkString(","))
    println(rdd1.subtract(rdd2).collect().mkString(","))
    println(rdd1.cartesian(rdd2).collect().mkString(","))

    println("Enter string:")
    val str = scala.io.StdIn.readLine()

  }
}
