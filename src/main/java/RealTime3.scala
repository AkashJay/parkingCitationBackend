import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils

object RealTime3 {

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
    val topicCount = "test"
    val topic1 = "real1"
    val topic2 = "real2"
    val topic3 = "real3"
    val topic4 = "real4"





    try {
      windowedStream1.foreachRDD(rdd => {
        if (!rdd.isEmpty()) {
          println("RDD row count: " + rdd.count())


         //---------------------01-----------------------------------

          val splitRdd1: RDD[Array[String]] = rdd.map(line => line.split(","))
          val yourRdd1: RDD[(String, String)] = splitRdd1.map(arr =>  (arr(0), arr(1)))
          val countRdd1: RDD[Int] = yourRdd1
            .groupBy( { case ( id, value ) => id } )  // group by title
            .map( { case ( title, iter ) => ( iter.size ) } )
          val record = new ProducerRecord[String, String](topic1, yourRdd1.count().toString)
          val metadata = producer.send(record)


          //---------------------02-----------------------------------
          val splitRdd2: RDD[Array[String]] = rdd.map(line => line.split(","))
          val yourRdd2: RDD[(String, Int)] = splitRdd2.map(arr =>  (arr(14), arr(16).toInt))
          val countRdd2: RDD[(String, Int)] = yourRdd2
            .groupBy( { case ( code, fine ) => code } )
            .mapValues(it => it.map(_._2).sum)
          for ((word) <- countRdd2.collect()) {
            if (word._2 > 50){
              val record = new ProducerRecord[String, String](topic2, word.toString)
              val metadata = producer.send(record)
            }

          }


          //---------------------03-----------------------------------

          val splitRdd3: RDD[Array[String]] = rdd.map(line => line.split(","))
          val yourRdd3: RDD[(String, String)] = splitRdd3.map(arr =>  (arr(14), arr(0)))
          val countRdd3: RDD[(String, Int)] = yourRdd3
            .groupBy( { case ( code, fine ) => code } )
            .map( { case ( title, iter ) => ( title, iter.size ) } )
          for ((word) <- countRdd3.collect()) {
            val record = new ProducerRecord[String, String](topic3, word.toString)
            val metadata = producer.send(record)
          }

          //---------------------04-----------------------------------

          val splitRdd4: RDD[Array[String]] = rdd.map(line => line.split(","))
          val yourRdd4: RDD[(String, Int)] = splitRdd4.map(arr =>  (arr(5), arr(16).toInt))
          val countRdd4: RDD[(String, Int)] = yourRdd4
            .groupBy( { case ( code, fine ) => code } )
            .mapValues(it => it.map(_._2).max)

          val a = countRdd4.reduceByKey((v1, v2) => v1)
          for ((word) <- a.collect()) {

              val record = new ProducerRecord[String, String](topic4, word.toString)
              val metadata = producer.send(record)

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
