import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.{CollectionAccumulator, LongAccumulator}
import java.util.Properties


object DBConnection {
  def main(args: Array[String]): Unit = {
    println("Testing the DB connection")

    //TODO Create spark connection
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark SQL data sources example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    import spark.implicits._
    runMysql(spark)
    spark.stop()
  }

  //FIXME
   private def runMysql(spark: SparkSession) : Unit = {
     val jdbcDF = spark.read
       .format("jdbc")
       .option("driver", "com.mysql.cj.jdbc.Driver")
       .option("url", "jdbc:mysql://localhost:3306/retail")
       .option("dbtable", "retail.invoice")
       .option("user", "root")
       .option("password", "welcome")
       .load()

       jdbcDF.show()
       val jdbcDF2 = jdbcDF.where("quantity > 200")
       jdbcDF2.show()


//       jdbcDF2.write
//         .format("jdbc")
//         .option("driver", "com.mysql.cj.jdbc.Driver")
//         .option("url", "jdbc:mysql://localhost:3306/retail")
//         .option("user", "root")
//         .option("password", "welcome")
//         .option("dbtable", "retail.invoice")
//         .insertInto("retail.invoice_rx")

//     val connectionProperties = new Properties()
//     connectionProperties.put("user", "root")
//     connectionProperties.put("password", "welcome")
//     val jdbcDF2 = spark.read
//       .jdbc("jdbc:mysql://localhost:3306/retail", "invoice", connectionProperties)

   }
}
