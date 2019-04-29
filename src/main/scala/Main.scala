import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.ConsumerGroupState
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import com.datastax.spark.connector._
import com.typesafe.config.ConfigFactory


import org.apache.log4j.{Level, Logger}



object main extends App{
  val logger = Logger.getLogger(this.getClass.getName)

  val appConfiguration = ConfigFactory.load()
  val master = appConfiguration.getString("common.spark.master")
  val cassandraUrl = appConfiguration.getString("common.cassandra.url")
  val cassandraFormat = appConfiguration.getString("common.cassandra.format")
  val kafka = appConfiguration.getString("common.kafka.url")

  val spark = SparkSession.builder()
    .appName("SparkStreamingTest")
    .master(master)
    .config("spark.cassandra.connection.host",cassandraUrl)
    .getOrCreate()
  val ssc = new StreamingContext(spark.sparkContext, Seconds(10))

  val kafkaParams = Map[String, Object](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> kafka,
    ConsumerConfig.GROUP_ID_CONFIG -> "groupe 1",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer])

  val topics = Set("spark")

  val messages = KafkaUtils.createDirectStream[String,String](ssc,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String,String](topics,kafkaParams))

  val lines = messages.map(_.value)
  //lines.print()
  lines.foreachRDD(x=>x.map(x=>{
   val arr =  x.split(":")
    (arr(0),arr(1).trim.toInt)
  }).saveToCassandra("sparkdata","spark_stream",SomeColumns("username", "age")))

  ssc.start()
  ssc.awaitTermination()


}