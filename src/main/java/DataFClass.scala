import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{
  IntegerType,
  LongType,
  Metadata,
  StringType,
  StructField,
  StructType
}

object DataFClass {

  def main(args: Array[String]): Unit = {

    val spark =
      SparkSession.builder().master("local").appName("Rajesh").getOrCreate()
    val sc = spark.sparkContext

    val rdd1 = sc.parallelize(List(1, 2, 3, 4))

    val myManualSchema = StructType(
      Array(
        StructField("DEST_COUNTRY_NAME", StringType, true),
        StructField("ORIGIN_COUNTRY_NAME", StringType, true),
        StructField(
          "count",
          IntegerType,
          false,
          Metadata.fromJson("{\"hello\":\"world\"}")
        )
      )
    )

    val df2 = spark.read
      .schema(myManualSchema)
      .json("data/flight-data/json/2015_rect.json")

    df2.show()
  }

}
