import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, count}

object PreProcess {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark =  SparkSession.builder().master("local").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val sparkdata = spark.read.format("csv").option("delimiter", ",").option("header", "true").load("In/parking-citations.csv")

    val tikcetIssuedByRoute = sparkdata.filter(col("Fine amount") =!= ""  || col("Fine amount") =!= null || col("Fine amount").isNotNull)
    tikcetIssuedByRoute.filter(col("Fine amount") === ""  || col("Fine amount") === null || col("Fine amount").isNull).show()

//    tikcetIssuedByRoute.repartition(1).write.mode(SaveMode.Overwrite)
//      .format("com.databricks.spark.csv")
//      .option("header", "true")
//      .save("In/newData.csv")
  }

}
