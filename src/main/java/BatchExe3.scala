import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, count, concat, lit}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object BatchExe3 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark =  SparkSession.builder().master("local").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val sparkdata: DataFrame = spark.read.format("csv").option("delimiter", ",").option("header", "true").load("In/parking-citations.csv")

    val groupbyCar = sparkdata.groupBy(col("Make") as "label")
                                  .agg(count(col("Ticket number")) as "value")
                                    .orderBy(col("value").desc).limit(10)
//    groupbyCar.repartition(1)
//      .write.mode(SaveMode.Overwrite)
//      .format("com.databricks.spark.csv")
//      .option("header", "true")
//      .save("In/groupbyCar.csv")

    val groupbyCarColor = sparkdata.groupBy(col("Make"), col("Color"))
                              .agg(count(col("Ticket number")) as "value")
                                .orderBy(col("value").desc).limit(5)

    val concatColorType = groupbyCarColor.select(concat(col("Make"), lit("_"), col("Color")) as "label", col("value"))

    concatColorType.repartition(1)
                  .write.mode(SaveMode.Overwrite)
                  .format("com.databricks.spark.csv")
                  .option("header", "true")
                  .save("In/groupbyCarColor.csv")





  }

}
