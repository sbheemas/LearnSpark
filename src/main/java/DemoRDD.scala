import org.apache.spark.sql.SparkSession

object DemoRDD {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("DemoRDD").getOrCreate()
    val sc = spark.sparkContext
    val rdd1 = sc.parallelize(List(1,2,3,4))

  }

}
