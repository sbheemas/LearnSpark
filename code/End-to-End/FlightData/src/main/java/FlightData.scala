//import jdk.nashorn.internal.runtime.regexp.joni.constants.StringType
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{
  IntegerType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.Encoders

object FlightData {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Hello World")
      .getOrCreate()

    val flightData2015 = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("data/flight-data/csv/2015-summary.csv")

    flightData2015.printSchema() //shows how spark has inferred the schema

    flightData2015.take(3).foreach(println) //Calls action take(3) on the DF.

    //Explain plan

    flightData2015.sort("count").explain()
    //Sort is a wide transformation as it has to operate on all the data in diff partitions
    // Exchange - rangePartitioning shows default 200 partitions

    spark.conf
      .set("spark.sql.shuffle.partitions", "5") // change the  partition to 5

    flightData2015
      .sort("count")
      .explain() // Now the partitions will show only 5

    flightData2015.sort("count").take(2).foreach(println)

    val retailEncoder = Encoders.product[FlightData]
    retailEncoder.printSchema

    val flightData2015 = spark.read
      .option("header", "true")
      .schema(FlightCase)
      .csv("D:\\UPSKILL\\LearnSpark\\data\\flight-data\\csv\\2010-summary.csv")
      .as[FlighCase]

  }

  case class FlightCase(DEST_COUNTRY_NAME: String,
                        ORIGIN_COUNTRY_NAME: String,
                        count: IntegerType)

}
