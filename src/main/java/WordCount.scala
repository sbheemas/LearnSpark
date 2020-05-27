import org.apache.spark.sql.SparkSession

object WordCount {
  def main(args: Array[String]): Unit = {
    //val sparkSession = SparkSession.builder().master("local").appName("Hello World").getOrCreate()

    val sparkSession = SparkSession.builder().appName("Test").master("local").getOrCreate()


      val sc = sparkSession.sparkContext

    val inputRdd = sc.textFile("D:\\dataset\\inputfile.txt")
    val wordRdd = inputRdd.flatMap(line=>line.split(" "))
    val pairRdd = wordRdd.map(word => (word,1)).reduceByKey((x,y) => x+y)
    pairRdd.collect().foreach(println)

    val oddEvenRDD = sc.textFile("D:\\UPSKILL\\HADOOP-MD\\evenodd.txt")
    val oddEvenRDD2 = oddEvenRDD.flatMap(line => line.split(",")).map(x => x.toInt)
    var even=0
    var odd = 0
    //val oddEvenRDD3 = oddEvenRDD2.filter( x => {if( x % 2 == 0)  {even+=x;  true } else {odd+=x ;  false}  } )
    println("Odd Even....")

    val o3 = oddEvenRDD2.map(x=> {if (x%2 == 0) {("EVEN",x)} else {("ODD",x)} })
    o3.reduceByKey((x,y)=> (x+y)).collect.foreach(println)
    //o3.foreach(println)





  }
}
