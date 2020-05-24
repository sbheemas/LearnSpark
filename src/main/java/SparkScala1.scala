import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession


import scala.io.StdIn

object SparkScala1 {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().master("local").appName("Hello World").getOrCreate()
    val sc = sparkSession.sparkContext

    val rdd1 = sc.textFile("D:\\UPSKILL\\HADOOP-MD\\spark_progs\\SparkScala1-master\\student.csv")

    rdd1.collect().foreach(println)
    println(rdd1.collect())

    val rdd2 = sc.parallelize(List(1,2,3,4,5,6))
    rdd2.collect().foreach(println)

    val rdd3 = rdd2.map(y =>(y * y))
    println("Map output:")
    rdd3.collect().foreach(println)
    println("rdd3: " + rdd3.collect().mkString(","))
    val rdd4 = rdd3.filter(x => (x%2 == 0))
    println(rdd4.collect().mkString(","))

    val rdd5 = sc.parallelize(List("Good Morning", "Hello World", "hi", "Hello India", "Karnataka India"))

    val rdd6 = rdd5.flatMap(x => x.split(" "))
    println(rdd6.collect().mkString(","))
    rdd6.distinct().collect().foreach(println)
    //rdd6.saveAsTextFile("C:\\Users\\Chandrika Sanjay\\RDDOutputFile")



    val rdd7 = sc.parallelize(List(1,2,3,4,5,6,7,8))
    println(rdd7.collect().mkString(","))



    println("Enter string:")
    val str = scala.io.StdIn.readLine()


  }
}
