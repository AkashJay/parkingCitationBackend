import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, count}

object BatchExe1 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark =  SparkSession.builder().master("local").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val sparkdata = spark.read.format("csv").option("delimiter", ",").option("header", "true").load("In/parking-citations.csv")

    val tikcetIssuedByRoute = sparkdata.groupBy(col("Route")).agg(count(col("Ticket number")))
    val tikcetIssuedByRouteFinal = tikcetIssuedByRoute.select(col("Route") as "label", col("count(Ticket number)") as "value")
                                                      .orderBy(col("value").desc).limit(10)

    tikcetIssuedByRoute.show(false)
    tikcetIssuedByRouteFinal.printSchema()
    tikcetIssuedByRouteFinal.repartition(1).write.mode(SaveMode.Overwrite)
          .format("com.databricks.spark.csv")
          .option("header", "true")
          .save("In/tikcetIssuedByRoute.csv")
  }

}
