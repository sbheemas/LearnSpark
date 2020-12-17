import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.KafkaUtils._

object KafkaWordCount {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Hello World")
      .getOrCreate()
    val sc = sparkSession.sparkContext
    val brokers = "localhost:9092"
    val topics = "08jul"
    val groupId = "group1"
    sc.setLogLevel("ERROR")
    val ssc = new StreamingContext(sc, Seconds(15))
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[
        StringDeserializer
      ],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
        classOf[StringDeserializer]
    )
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams)
    )
    messages
      .map(_.value)
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey(_ + _)
      .print()
    ssc.start()
    ssc.awaitTermination()
  }
}
