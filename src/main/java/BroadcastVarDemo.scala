import org.apache.spark.{SparkContext, rdd}
import org.apache.spark.sql.SparkSession
//read only
object BroadcastVarDemo {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Hello World")
      .getOrCreate()
    val sc = sparkSession.sparkContext

    broadcastVarInt(sc)
    broadcastVarList(sc)

  }

  def broadcastVarInt(sc: SparkContext) = {
    val rdd1 = sc.parallelize(List(1, 2, 3, 4, 5, 6))
    rdd1.map(x => x * 5).collect
    val bc = sc.broadcast(5)
    println(bc.value)
    println(rdd1.map(x => x * bc.value).collect.mkString(","))
  }

  def broadcastVarList(sc: SparkContext) = {
    val list1 = List("a", "d", "f")
    val bc = sc.broadcast(list1)
    val rdd1 =
      sc.parallelize(List("a", "b", "c", "d", "e", "f", "g", "a", "b", "c"))
    println(rdd1.filter(x => bc.value.contains(x)).collect.mkString(","))
  }
}
