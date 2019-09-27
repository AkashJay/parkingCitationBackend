import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object sturucturedStreaming {

  def main(args: Array[String]): Unit = {

    println("dvsdvsdfvsdfv")
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession
      .builder
      .appName("Spark-Kafka-Integration")
      .master("local")
      .getOrCreate()

    val mySchema = StructType(Array(
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("year", IntegerType),
      StructField("rating", DoubleType),
      StructField("duration", IntegerType)
    ))
    val streamingDataFrame: DataFrame = spark.readStream.schema(mySchema).csv("/home/akash/Downloads/test/")

    streamingDataFrame.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value").
      writeStream
      .format("kafka")
      .option("topic", "testingNew")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("checkpointLocation", "/home/akash/Documents/ttt")
      .start()


    println("111111111111111111111")

    import spark.implicits._
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "testingNew")
      .load()

    println("2222222222222222")

    val df1 = df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)").as[(String, Timestamp)]
      .select(from_json($"value", mySchema).as("data"), $"timestamp")
      .select("data.*", "timestamp")

    df1.writeStream
      .format("console")
      .option("truncate","false")
      .start()
      .awaitTermination()

  }
}
