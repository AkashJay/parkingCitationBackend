import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils

object RealTime2 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)


    val conf = new SparkConf().setMaster("local[*]").setAppName("Wordcount")
    val ssc = new StreamingContext(conf, Seconds(2))
    //    ssc.checkpoint("checkpoint")

    //    val kafkaParam = Map("metadata.broker.list"-> "localhost:9092")
    val topics = "testingNew"

    val zkQuorum = "localhost:2181"
    val group = "g1"

    val mySchema = StructType(Array(
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("year", IntegerType),
      StructField("rating", DoubleType),
      StructField("duration", IntegerType)
    ))


    //    val Array(zkQuorum, group, topics, numThreads) = args



    val topicMap = topics.split(",").map((_, 1)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    val windowedStream1: DStream[String] = lines.window(Seconds(10))



    val props:Properties = new Properties()
    props.put("bootstrap.servers","localhost:9092")
    props.put("key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks","all")
    val producer = new KafkaProducer[String, String](props)
    val topicAverage = "averageFine"




    try {
      windowedStream1.foreachRDD(rdd => {
        if (!rdd.isEmpty()) {
          println("RDD row count: " + rdd.count())

          val splitRdd2: RDD[Array[String]] = rdd.map(line => line.split(","))
          val yourRdd2: RDD[(String, Int)] = splitRdd2.map(arr =>  (arr(14), arr(16).toInt))
          val countRdd2: RDD[(String, Int)] = yourRdd2
            .groupBy( { case ( code, fine ) => code } )
            .mapValues(it => it.map(_._2).sum)
          for ((word) <- countRdd2.collect()) {
            if (word._2 > 50){
              val record = new ProducerRecord[String, String](topicAverage, word.toString)
              val metadata = producer.send(record)
            }

          }

        }
      }
      )
    }catch{
      case e:Exception => e.printStackTrace()
    }finally {
    }




    ssc.start()
    ssc.awaitTermination()
  }




}
