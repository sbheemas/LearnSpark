import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.{CollectionAccumulator, LongAccumulator}

//write only

object AccumulatorDemo {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().master("local").appName("Hello World").getOrCreate()
    val sc = sparkSession.sparkContext

    val rdd1 = sc.parallelize( List( "Apache Spark is a unified analytics engine for large-scale data processing"))
    val acc = sc.longAccumulator("Count of words")



    rdd1.flatMap ( x => x.split(" ")).foreach(x => acc.add(1))
    println(acc.value)
    //longAccDemo(sc)



    collectionAccDemo(sc)


    println("Enter string:")
    scala.io.StdIn.readLine()

  }

  def collectionAccDemo(sc:SparkContext) :Unit = {
    println("Collection Accumulator Demo")
    val collectionAcc = new CollectionAccumulator[String]
    sc.register(collectionAcc, "Strings having more than 5 letters")
    val rdd4 = sc.textFile("D:\\UPSKILL\\HADOOP-MD\\spark_progs\\SparkScala1-master\\lockdown.txt")

    def collAdd(x:String) : Unit = { if ( x.length > 6 ) {collectionAcc.add(x) ; println(x)}}
    rdd4.flatMap(x => x.split(" ")).foreach( x => collAdd(x))
    println(collectionAcc.value)
  }

  def longAccDemo(sc: SparkContext ) :Unit = {
    println("Long Accumulator Demo")
    // One other method of initialising accumulator
    //val longAcc = new LongAccumulator
    //sc.register(longAcc, "Count of words having more than 10 letters")
    val longAcc = sc.longAccumulator("Count of words having more than 5 letters")
    val rdd4 = sc.textFile("D:\\UPSKILL\\HADOOP-MD\\spark_progs\\SparkScala1-master\\lockdown.txt")

    def collAdd(x:String) : Unit = { if ( x.length > 5 ) {longAcc.add(1) ; println(x)}}
    rdd4.flatMap(x => x.split(",")).foreach( x => collAdd(x))
    println(longAcc.value)
    // longAcc.reset  to reset the accumulator
  }

}
