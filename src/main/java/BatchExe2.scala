import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, count}

object BatchExe2 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark =  SparkSession.builder().master("local").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val sparkdata = spark.read.format("csv").option("delimiter", ",").option("header", "true").load("In/parking-citations.csv")


    val exe2 = sparkdata.groupBy(col("Violation Description"), col("Route"))
      .agg(count(col("Ticket number")))
    val topViolations: DataFrame = exe2.select(col("Route") as "rowid", col("Violation Description") as "columnid", col("count(Ticket number)") as "value")



    val newa = topViolations.filter(col("columnid") === "NO PARK/STREET CLEAN" || col("columnid") === "METER EXP." || col("columnid") === "RED ZONE" || col("columnid") === "PREFERENTIAL PARKING" || col("columnid") === "DISPLAY OF TABS")
      .filter(col("rowid") === "00600" || col("rowid") === "00500" || col("rowid") === "00402" || col("rowid") === "00401")
      .orderBy(col("rowid"), col("columnid"))

    newa.repartition(1).write.mode(SaveMode.Overwrite)
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save("In/newa.csv")

    newa.repartition(1)
          .write.mode(SaveMode.Overwrite)
          .format("com.databricks.spark.csv")
          .option("header", "true")
          .save("In/topViolations.csv")
  }

}
