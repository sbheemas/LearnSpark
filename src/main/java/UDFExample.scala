import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
object UDFExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Hello World")
      .getOrCreate()
    val mulUDF = udf(mul(_: Int): Int)
    val df = spark.read.option("header", "true").csv("data\\student.csv")

    df.select(mulUDF(col("id"))).show
  }
  def mul(x: Int): Int = x * 5
}
