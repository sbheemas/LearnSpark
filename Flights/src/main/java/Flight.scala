import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object Flight {
  def main(args: Array[String]): Unit = {
    //val spark = SparkSession.builder().master("local").appName("Spark Submit Demo").getOrCreate()
    val spark = SparkSession.builder().master(args(1)).appName("Spark Submit Demo").getOrCreate()
    val sc = spark.sparkContext

    //val df = spark.read.json("..\\..\\data\\flight-data\\json\\2015-summary.json")
    //val dataset_path = "..\\..\\data\\flight-data\\json\\2015-summary.json"

    //args(0) is the path to your data file
    //args(1) is the master option - You can provide local or yarn
    /*
    # How to Submit the jar in our local lap top
       spark-submit --class Flight --master local Flights-1.0.jar ..\\..\\data\\flight-data\\json\\2015-summary.json local

      # the above jar takes 2 arguments - dataset path and master mode.
    */

    val dataset_path = args(0)
    val df = spark.read.json(dataset_path)

    val df2 = df.where(col("dest_country_name") === "United States").select(count("dest_country_name"))
    df.createOrReplaceGlobalTempView("dfTable")

    spark.sql("select count(dest_country_name) from global_temp.dfTable where dest_country_name = \"United States\" ").show(4)

    //df2.show(4)
  }
}
