import org.apache.spark.sql.SparkSession

object JoinsDemo {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().master("local").appName("Hello World").getOrCreate()
    val sc = sparkSession.sparkContext


    val rdd1 = sc.parallelize(List(("A", 20), ("B", 30), ("C", 10), ("D", 40),("A", 50)))
    val rdd2 =  sc.parallelize(List(("A", 50), ("B", 30), ("C", 60), ("E", 80)))
    val rdd300 = sc.parallelize(List(("A", 50), ("B", 30), ("C", 60), ("E", 80)))

    println("Join:")
    rdd1.join(rdd2).collect().foreach(println)
    println("LeftOuterJoin:")
    rdd1.leftOuterJoin(rdd2).collect().foreach(println)
    println("RightOuterJoin:")
    rdd1.rightOuterJoin(rdd2).collect().foreach(println)
    println("FullOuterJoin:")
    rdd1.fullOuterJoin(rdd2).collect().foreach(println)
    println("SubtractByKey:")
    rdd1.subtractByKey(rdd2).collect().foreach(println)
    println("Cogroup:")
    //rdd1.cogroup(rdd2).collect().foreach(println)
    rdd1.cogroup(rdd2,rdd300).foreach(println)

    val rdd3 = sc.parallelize(List(("A", 20),
      ("B", 30),
      ("C", 50),
      ("C", 10),
      ("D", 40),
      ("B", 40),
      ("A", 60),
      ("D",50)))

    val var1 = 5

    val y = 5

    val bc_var = sc.broadcast(var1)
    println("Multiplying values by 5 :")
    rdd3.mapValues(x => x * bc_var.value).collect().foreach(println)


    val createCombiner = (x :Int) => ( x,1)
    val mergeValue = (acc: (Int,Int), v:Int) => (acc._1 + v, acc._2 + 1)
    val mergeCombiner = ( acc1: (Int,Int) , acc2:(Int,Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)

    rdd3.combineByKey(
      createCombiner,
      mergeValue,
      mergeCombiner
    ).mapValues(x => x._1 / x._2).collect().foreach(println)

    println(rdd3.collectAsMap().mkString(","))
  }

}
