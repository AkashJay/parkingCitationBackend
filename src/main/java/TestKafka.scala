import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext, kafka}
import _root_.kafka.serializer.StringDecoder
import org.apache.spark.sql.{DataFrame, SparkSession}
import DataSource.sparkSession
import KafkaProducerApp.{producer, topic}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, concat, count, lit}

object TestKafka {
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
    val words: DStream[String] = windowedStream1.flatMap(_.split(","))
    val wordCounts: DStream[(String, Long)] = words.map(x => (x, 1L))
                          .reduceByKey(_ + _)

//    windowedStream1.print()
//    words.print()
//    wordCounts.print()


    val props:Properties = new Properties()
    props.put("bootstrap.servers","localhost:9092")
    props.put("key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks","all")
    val producer = new KafkaProducer[String, String](props)
    val topicCount = "test"



    try {
      windowedStream1.foreachRDD(rdd => {
        if (!rdd.isEmpty()) {
          println("RDD row count: " + rdd.count())

          val splitRdd1: RDD[Array[String]] = rdd.map(line => line.split(","))
          val yourRdd1: RDD[(String, String)] = splitRdd1.map(arr =>  (arr(0), arr(1)))


          val countRdd1: RDD[Int] = yourRdd1
            .groupBy( { case ( id, value ) => id } )  // group by title
            .map( { case ( title, iter ) => ( iter.size ) } )

//          for ((word) <- countRdd.collect()) {
            val record = new ProducerRecord[String, String](topicCount, yourRdd1.count().toString)
            val metadata = producer.send(record)
//          }

        }
      }
      )
    }catch{
      case e:Exception => e.printStackTrace()
    }finally {
      //          producer.close()
    }




    ssc.start()
    ssc.awaitTermination()
  }



}


