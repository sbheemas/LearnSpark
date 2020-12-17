package main.java

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object CheckmeJSON {

  def main(args: Array[String]): Unit = {
    println("Apache Spark Application Started ...")

    val spark1 = SparkSession
      .builder()
      .master("local")
      .appName("Validate JSON file")
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
    val jsonDF = spark1.read.json(
      "D:\\UPSKILL\\spark-power\\sdf-converter\\sdf\\something2.json"
    )

    jsonDF.show(false)
    println("Application ended")
  }

}
