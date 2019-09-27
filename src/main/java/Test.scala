import org.apache.log4j.{Level, Logger}
import org.apache.spark._
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext

object Test {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf().setMaster("local[*]").setAppName("Wordcount")
    val ssc = new StreamingContext(conf, Seconds(2))
    val lines = ssc.socketTextStream("localhost", 9092)

    val words = lines.flatMap(_.split(" "))

    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)


    // Print the first ten elements of each RDD generated in this DStream to the console
    println("fvbfs")
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()



  }

}
