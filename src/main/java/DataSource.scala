import org.apache.spark.sql.SparkSession

object DataSource {
  val sparkSession = SparkSession.builder().appName("RddToDataframe").master("local[*]").getOrCreate()
}
