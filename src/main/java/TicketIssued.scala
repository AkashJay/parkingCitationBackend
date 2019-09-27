import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.functions.{col, _}
import java.util.Date
import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.temporal.ChronoUnit
import scala.collection.mutable.LinkedHashSet

import scala.math.exp

object TicketIssued {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark =  SparkSession.builder().master("local").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val sparkdata = spark.read.format("csv").option("delimiter", ",").option("header", "true").load("In/parking-citations.csv")
  // sparkdata.filter(col("Violation code") === "88.13B+").show()

    val tikcetIssuedByRoute = sparkdata.groupBy(col("Route")).agg(count(col("Ticket number")))
    val tikcetIssuedByRouteFinal = tikcetIssuedByRoute.select(col("Route") as "label", col("count(Ticket number)") as "value").orderBy(col("value").desc).limit(10)
//    tikcetIssuedByRoute.orderBy(col("count(Ticket number)").desc).show()
    //tikcetIssuedByRoute.show(false)
//    tikcetIssuedByRouteFinal.printSchema()
//    tikcetIssuedByRouteFinal.repartition(1)
//      .write.mode(SaveMode.Overwrite)
//      .format("com.databricks.spark.csv")
//      .option("header", "true")
//      .save("In/tikcetIssuedByRoute.csv")


    //---------------------------exercise 02----------------------------------
    val exe21 = sparkdata.groupBy(col("Violation Description"))
      .agg(count(col("Ticket number")))
   // exe21.orderBy(col("count(Ticket number)").desc).limit(20).show(false)


    val exe2 = sparkdata.groupBy(col("Violation Description"), col("Route"))
                      .agg(count(col("Ticket number")))

    //exe2.show()




    val topViolations: DataFrame = exe2.select(col("Route") as "rowid", col("Violation Description") as "columnid", col("count(Ticket number)") as "value")



    val filter1 = topViolations.filter(col("columnid") === "NO PARK/STREET CLEAN" && col("rowid") === "00600")
    val filter2 = topViolations.filter(col("columnid") === "METER EXP." && col("rowid") === "00600")
    val filter3 = topViolations.filter(col("columnid") === "RED ZONE" && col("rowid") === "00600")
    val filter4 = topViolations.filter(col("columnid") === "PREFERENTIAL PARKING" && col("rowid") === "00600")
    val filter5 = topViolations.filter(col("columnid") === "DISPLAY OF TABS" && col("rowid") === "00600")

    val filter11 = topViolations.filter(col("columnid") === "NO PARK/STREET CLEAN" && col("rowid") === "00500")
    val filter21 = topViolations.filter(col("columnid") === "METER EXP." && col("rowid") === "00500")
    val filter31 = topViolations.filter(col("columnid") === "RED ZONE" && col("rowid") === "00500")
    val filter41 = topViolations.filter(col("columnid") === "PREFERENTIAL PARKING" && col("rowid") === "00500")
    val filter51 = topViolations.filter(col("columnid") === "DISPLAY OF TABS" && col("rowid") === "00500")

    val filter12 = topViolations.filter(col("columnid") === "NO PARK/STREET CLEAN" && col("rowid") === "00402")
    val filter22 = topViolations.filter(col("columnid") === "METER EXP." && col("rowid") === "00402")
    val filter32 = topViolations.filter(col("columnid") === "RED ZONE" && col("rowid") === "00402")
    val filter42 = topViolations.filter(col("columnid") === "PREFERENTIAL PARKING" && col("rowid") === "00402")
    val filter52 = topViolations.filter(col("columnid") === "DISPLAY OF TABS" && col("rowid") === "00402")

    val filter13 = topViolations.filter(col("columnid") === "NO PARK/STREET CLEAN" && col("rowid") === "00401")
    val filter23 = topViolations.filter(col("columnid") === "METER EXP." && col("rowid") === "00401")
    val filter33 = topViolations.filter(col("columnid") === "RED ZONE" && col("rowid") === "00401")
    val filter43 = topViolations.filter(col("columnid") === "PREFERENTIAL PARKING" && col("rowid") === "00401")
    val filter53 = topViolations.filter(col("columnid") === "DISPLAY OF TABS" && col("rowid") === "00401")

    val dfs = Seq(filter1,filter2,filter3,filter4,filter5,filter11,filter21,filter31,filter41,filter51,
      filter12,filter22,filter32,filter42,filter52,filter13,filter23,filter33,filter43,filter53)

//    dfs.reduce(_ union _).repartition(1)
//      .write.mode(SaveMode.Overwrite)
//      .format("com.databricks.spark.csv")
//      .option("header", "true")
//      .save("In/topViolations.csv")

//    sparkdata.select(col("Make")).distinct().show()

   // dfs.reduce(_ union _).show(false)
//
//    val newa = topViolations.filter(col("columnid") === "NO PARK/STREET CLEAN" || col("columnid") === "METER EXP." || col("columnid") === "RED ZONE" || col("columnid") === "PREFERENTIAL PARKING" || col("columnid") === "DISPLAY OF TABS")
//      .filter(col("rowid") === "00600" || col("rowid") === "00500" || col("rowid") === "00402" || col("rowid") === "00401")
//      .orderBy(col("rowid"), col("columnid"))
//
//    newa.repartition(1).write.mode(SaveMode.Overwrite)
//      .format("com.databricks.spark.csv")
//      .option("header", "true")
//      .save("In/newa.csv")




    // ee.orderBy(col("count(Ticket number)").desc).show()
    //topViolations.show(false)


//    topViolations.show(false)
//    topViolations.show(false)

//    topViolations.repartition(1)
//      .write.mode(SaveMode.Overwrite)
//      .format("com.databricks.spark.csv")
//      .option("header", "true")
//      .save("In/topViolations.csv")


//    fineCollectedInMillion.repartition(1)
//      .write.mode(SaveMode.Overwrite)
//      .format("com.databricks.spark.csv")
//      .option("header", "true")
//      .save("In/fineCollected.csv")

    


  }

  def convertToMillion = udf((total: Int) => {
    (total/1000000).toDouble
  })

}
