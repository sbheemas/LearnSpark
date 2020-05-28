import org.apache.spark.sql.SparkSession

object ExceptionHandlingTest {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .master("local")
      .appName("ExceptionHandlingTest")
      .getOrCreate()
    println("Executing ....")
    spark.sparkContext.parallelize(0 until spark.sparkContext.defaultParallelism).foreach { i =>
      if (math.random > 0.75) {
        throw new Exception("Testing exception handling")
      }
    }

    spark.stop()
  }
}
