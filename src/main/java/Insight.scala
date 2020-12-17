import org.apache.spark.sql.functions.{window, col, column, explode}
import org.apache.spark.sql.types.{
  ArrayType,
  DateType,
  FloatType,
  IntegerType,
  StringType,
  StructField,
  StructType
}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import flattenStruct._
//import com.google.auth.oauth2.ServiceAccountCredentials

object Insight {

  def main(args: Array[String]): Unit = {

    println("Apache Spark Application Started ...")

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Insights for proSpark")
      .getOrCreate()

    val retailSchema = StructType(
      Array(
        StructField(
          "_m",
          StructType(
            Array(
              StructField("_rt", StringType),
              StructField("_src", StringType),
              StructField("_o", StringType),
              StructField("src_dtls", StringType)
            )
          )
        ),
        StructField(
          "_p",
          StructType(
            Array(
              StructField(
                "data",
                StructType(
                  Array(
                    StructField("InvoiceNo", StringType),
                    StructField("StockCode", StringType),
                    StructField("Description", StringType),
                    StructField("Quantity", StringType),
                    StructField("InvoiceDate", StringType),
                    StructField("UnitPrice", StringType),
                    StructField("CustomerID", StringType),
                    StructField("Country", StringType)
                  )
                )
              )
            )
          )
        )
      )
    )

    val finalSchema = StructType(
      Array(
        StructField("InvoiceNo", StringType),
        StructField("StockCode", StringType),
        StructField("Description", StringType),
        StructField("Quantity", IntegerType),
        StructField("InvoiceDate", DateType),
        StructField("UnitPrice", FloatType),
        StructField("CustomerID", StringType),
        StructField("Country", StringType)
      )
    )

//    val finalfinalSchema = StructType(Array(
//      StructField("_rt", StringType),
//      StructField("_src", StringType),
//      StructField("_o", StringType),
//      StructField("InvoiceNo", StringType),
//      StructField("StockCode", StringType),
//      StructField("Description",StringType),
//      StructField("Quantity", IntegerType),
//      StructField("InvoiceDate", DateType),
//      StructField("UnitPrice", FloatType),
//      StructField("CustomerID",StringType),
//      StructField("Country",StringType))
//    )
    val jsonInsightDF = spark.read
      .option("multiline", true)
      .schema(retailSchema)
      .json(
        "D:\\UPSKILL\\Spark-The-Definitive-Guide\\data\\retail-data\\by-day\\2011-01-05.json"
      )

    val final_df = jsonInsightDF.select(col("_p.data.*"))

    final_df.printSchema()

    //    val insightFlatDF = jsonInsightDF.select(flattenStructSchema(jsonInsightDF.schema):  _*)
    //    println("O/p of flattenStructSchema")
    //    insightFlatDF.show(5)
    val bucket = "sankir-storage-prospark"
    spark.conf.set("temporaryGcsBucket", bucket)

    val pro_spark_key_path =
      "D:\\UPSKILL\\spark-power\\sdf-converter\\pro-spark-94d27b0094ec.json"
    spark.conf.set("credentialsFile", pro_spark_key_path)

    final_df.createOrReplaceGlobalTempView("retail_tbl")
    println(
      "KPI1: Highest selling SKUs on a daily basis (M,T,W,Th,F,S,Su) per country"
    )
    spark.sql(
      """select distinct sum(round(quantity * unitprice)) over w as revenue,
                          stockcode,
                          dayofweek(InvoiceDate) as Day_Of_Week, country
                          from global_temp.retail_tbl
                          window w as (partition by stockcode order by dayofweek(InvoiceDate),country)
                          order by  revenue desc, Day_Of_Week,country"""
    )
//      .write.format("bigquery")
//      .option("credentialsFile", pro_spark_key_path)
//      .option("table", "pro-spark:insights.stockcode_dow_daily")

//
    println("KPI2: Rank the SKUs based on the revenue - Worldwide")
    println("----------------------------------------------------")
    spark.sql("""select stockcode, sum(quantity * unitprice) as revenue ,
                         rank() over ( order by sum(quantity * unitprice) desc ) as ranking
                         from global_temp.retail_tbl
                         group by stockcode""").show(10, false)
//
//    println("KPI2: Rank the SKUs based on the revenue - Countrywide")
//    println("------------------------------------------------------")
//    spark.sql("""select stockcode, country, sum(quantity * unitprice) as revenue ,
//                         rank() over ( order by sum(quantity * unitprice) desc ) as ranking
//                         from global_temp.retail_tbl
//                         group by stockcode, country""").show(10, false)
//
//    println("KPI3: Identify Sales Anomalies in any month for a given SKU")
//    println("-----------------------------------------------------------")
//    spark.sql("""select stockcode, year(invoicedate) as Year, month(invoicedate)  as Month, sum(quantity * unitprice) as Revenue
//                         from global_temp.retail_tbl
//                         group by stockcode, year(invoicedate), month(invoicedate)
//                         order by stockcode, year(invoicedate), month(invoicedate) """).show(10,false)
//
//    println("KPI4: Rank the most valuable to least valuable customers")
//    println("--------------------------------------------------------")
//    spark.sql("""select customerid, sum(quantity * unitprice) as revenue,
//                         rank() over ( order by sum(quantity * unitprice) desc) as Most_valuable_rank
//                         from global_temp.retail_tbl
//                         group by customerid""").show(5,false)
//
//    println("KPI5: Rank the highest revenue to lowest revenue generating countries")
//    println("---------------------------------------------------------------------")
//    spark.sql("""select country, sum(quantity * unitprice),
//                         rank() over ( order by sum(quantity * unitprice) desc) as Ranking
//                         from global_temp.retail_tbl
//                         group by country""").show(5, false)
//
//
//    println("KPI6: Revenue per SKU - Quarterwise")
//    println("-----------------------------------")
//    spark.sql("""select stockcode, year(invoicedate), quarter(invoicedate) , sum(quantity * unitprice) as revenue
//                          from global_temp.retail_tbl
//                          group by stockcode, year(invoicedate), quarter(invoicedate)
//                          order by stockcode,year(invoicedate), quarter(invoicedate)""").show(5, false)
//
//    println("KPI7: Revenue per country - QTR")
//    println("-------------------------------")
//    spark.sql("""select country, year(invoicedate),
//                         quarter(invoicedate) , sum(quantity * unitprice) as revenue
//                         from global_temp.retail_tbl
//                         group by country, year(invoicedate), quarter(invoicedate)
//                         order by country, year(invoicedate), quarter(invoicedate) """).show(5, false)

    spark.stop()
    println("Apache Spark Application Completed.")
  }

}
