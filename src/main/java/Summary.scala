import TicketIssued.convertToMillion
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Summary {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark =  SparkSession.builder().master("local").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val sparkdata = spark.read.format("csv").option("delimiter", ",").option("header", "true").load("In/parking-citations.csv")
    val totalRecord = sparkdata.agg(count(col("Ticket number")) as "totRec").withColumn("totalRecord", convertToMillionR(col("totRec")))
//    totalRecord.show()

    totalRecord.repartition(1).write.mode(SaveMode.Overwrite)
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save("In/totalRecord.csv")


    val yearFifteen = sparkdata.filter(col("Issue Date").startsWith("2015")).agg(count(col("Issue Date")) as "2015").withColumn("totalRecord15", convertToMillionR(col("2015")))
//    yearFifteen.show()
    yearFifteen.repartition(1).write.mode(SaveMode.Overwrite)
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save("In/yearFifteen.csv")


    val thisYRec = sparkdata.filter(col("Issue Date").startsWith("2016")).agg(count(col("Issue Date")) as "2016").withColumn("totalRecord16", convertToMillionR(col("2016")))
//    thisYRec.show()

    val yeaeSeventyeen = sparkdata.filter(col("Issue Date").startsWith("2017")).agg(count(col("Issue Date")) as "2017").withColumn("totalRecord17", convertToMillionR(col("2017")))
//    yeaeSeventyeen.show()

    yeaeSeventyeen.repartition(1).write.mode(SaveMode.Overwrite)
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save("In/yeaeSeventyeen.csv")

    val b = sparkdata.filter(col("Issue Date").startsWith("2018")).agg(count(col("Issue Date")) as "2018").withColumn("totalRecord18", convertToMillionR(col("2018")))
//    b.show()


    val fineCollected: DataFrame = sparkdata.agg(sum(col("Fine amount")) as "totalFine")
    val fineCollectedInMillion = fineCollected.withColumn("socre", convertToMillionF(col("totalFine")))
//    fineCollectedInMillion.show()

    fineCollectedInMillion.repartition(1).write.mode(SaveMode.Overwrite)
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save("In/fineCollectedInMillion.csv")
  }

  def convertToMillionR = udf((total: Double) => {
//    (total/1000000).toDouble
    BigDecimal((total/1000000)).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble
  })

  def convertToMillionF = udf((total: Double) => {
    //    (total/1000000).toDouble
    BigDecimal((total/1000000)).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble
  })

}
