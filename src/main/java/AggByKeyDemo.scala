import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object AggByKeyDemo {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Hello World")
      .getOrCreate()
    val sc = sparkSession.sparkContext

    //val a=0
    //val acc = sc.longAccumulator("a")

    //List of unique values for a key
    val rdd1 = sc.parallelize(
      List(
        ("Bangalore", "A"),
        ("Bangalore", "B"),
        ("Bangalore", "C"),
        ("Bangalore", "B"),
        ("Bangalore", "A"),
        ("Bangalore", "D"),
        ("Bangalore", "B"),
        ("Bangalore", "E"),
        ("Bangalore", "B"),
        ("Bangalore", "A"),
        ("Mumbai", "B"),
        ("Mumbai", "A"),
        ("Mumbai", "C"),
        ("Mumbai", "A"),
        ("Mumbai", "B"),
        ("Mumbai", "B"),
        ("Delhi", "D"),
        ("Delhi", "B"),
        ("Delhi", "C"),
        ("Delhi", "A"),
        ("Delhi", "A")
      ),
      3
    )

    println("List of unique values for a key:")
    val initialSet = mutable.HashSet.empty[String]
    val mergeValue = (acc: mutable.HashSet[String], v: String) => acc += v
    val mergePartition =
      (p1: mutable.HashSet[String], p2: mutable.HashSet[String]) => p1 ++= p2
    val rdd2 = rdd1.aggregateByKey(initialSet)(mergeValue, mergePartition)
    rdd2.collect().foreach(println)

    // Count of values for a particular key , regardless of the value

    println("Count of values for a particular key:")
    //val rdd3 = rdd1.aggregateByKey(0)(( count:Int, v : String) => count + 1 ,( acc1 : Int, acc2 :Int) => acc1 + acc2 )
    val rdd3 = rdd1.aggregateByKey(0)(
      (count: Int, v: String) => count + 1,
      (acc1: Int, acc2: Int) => acc1 + acc2
    )
    rdd3.collect().foreach(println)

    println(rdd1.countByKey().mkString(","))

    println(rdd1.sortByKey().collect().mkString(","))

    println("collectAsMap")
    val map1 = rdd1.sortByKey().collectAsMap()
    println(map1)
    for ((k, v) <- map1) printf("key: %s, value: %s\n", k, v)

    println(rdd1.lookup("Delhi"))

    val acc_val = sc.longAccumulator("acc_val")

    val rdd4 = sc.textFile("data\\student.txt")

    def filterMethod(x: String): Boolean = {
      if (x.length < 5) {
        acc_val.add(1); return false
      } else return true;
    }

    val rdd5 = rdd4.flatMap(x => x.split(" ")).filter(x => filterMethod(x))

    rdd4
      .flatMap(x => x.split(" "))
      .filter(x => filterMethod(x))
      .saveAsTextFile("C:\\FILTERED_TEXT_FILE1")

    println("Number of words starting from i : " + acc_val.value)

  }
}
