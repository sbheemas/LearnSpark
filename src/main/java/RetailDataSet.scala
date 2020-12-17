import org.apache.spark.sql.{SaveMode, SparkSession}

object RetailDataSet {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Retail-DataSet")
      .getOrCreate()

    val df = spark.read.option("header", "true").csv("data\\retail-data")

    df.show()

    df.printSchema()

    df.createTempView("retail_data")

    val kpi1 = spark.sql(
      "select Country, sum(Quantity) from retail_data group by Country order by sum(Quantity) desc"
    )

    //kpi1.show()

    kpi1.coalesce(1).write.mode(SaveMode.Append).csv("C:\\kpi1")

    val kpi2 = spark.sql(
      "select CustomerID, sum(Quantity) from retail_data where CustomerID != null group by CustomerID order by sum(Quantity) desc"
    )

    //kpi2.show()

    kpi2.coalesce(1).write.mode(SaveMode.Append).csv("C:\\kpi2")

  }
}
