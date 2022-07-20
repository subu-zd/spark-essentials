package part_2_spark_dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._
import part_2_spark_dataframes.DataFramesBasics.spark

object DataSources extends App {
  val spark = SparkSession.builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  /* Reading a DF:
  * - format
  * - schema (optional) or inferSchema - true
  * - path
  * - zero or more options
  * */
  val carsDF = spark.read
    .format("json")
    .schema(carsSchema) // enforce a schema
    .option("mode", "failFast") // mode decides what Spark should do in case it encounters a malformed record.
    // e.g. if a JSON doesn't conform to the schema or if the JSON is simply malformed
    // options for mode - failFast (throws an exception if we encounter malformed data), dropMalformed (ignores faulty rows), permissive (default)
    .option("path", "src/main/resources/data/cars.json")
    .load()

  carsDF.show() // action

  // massing options in a map gives us the advantage of computing options dynamically at runtime
  val carsDFWithOptionMap = spark.read
    .format("json")
    .options(Map(
      "mode" -> "failFast",
      "path" -> "src/main/resources/data/cars.json",
      "inferSchema" -> "true"
    ))

  /* Writing DFs
  * - format
  * - save mode : overwrite, append, ignore, errorIfExists (these values specify what spark should do if the file we're writing to already exists)
  * - path
  * - zero or more options
  * */
  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars_dupe.json")

}
