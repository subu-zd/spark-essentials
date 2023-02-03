package part_3_types_and_datasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexTypes extends App {

  val spark = SparkSession.builder()
    .appName("Complex Data Types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // Dates
  val moviesWithReleaseDates = moviesDF
    .select(col("Title"), to_date(col("Release_Date"), "d-MMM-yy").as("Actual_Release")) // conversion

  moviesWithReleaseDates
    .withColumn("Today", current_date())
    .withColumn("Right_Now", current_timestamp())
    .withColumn("Movie age", datediff(col("Today"), col("Actual_Release")) / 365) // date_add, date_sub
    .show()

  moviesWithReleaseDates.select("*").where(col("Actual_Release").isNull).show()

  /*
  * Exercise
  * 1. How do we deal with changing date formats?
  * 2. Read dates from the stock.csv
  * */

  // 1.
  // parse the data frame multiple times with all the possible date formats, then union the small DFs
  // this is an essential task for data cleaning
  // obvious trade off is the compute overhead of parsing the data frame multiple times
  // a workaround to this problem is ignoring the values that don't conform to the format


  // 2.
  val stocksDF = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/data/stocks.csv")

  val stocksWithFormattedDate = stocksDF.select(col("symbol"), to_date(col("date"), "MMM d yyyy").as("date"), col("price"))

  stocksWithFormattedDate.show()

  // structures

  // 1 - with col operators
  moviesDF
    .select(col("Title"), struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit"))
    .select(col("Title"), col("Profit").getField("US_Gross").as("US_Profit"))

  // 2 - with expression strings
  moviesDF.selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")
    .selectExpr("Title", "Profit.US_Gross")

  // Arrays
  val moviesWithWords = moviesDF.select(col("Title"), split(col("Title"), " |,").as("Title_Words")) // ARRAY of string

  moviesWithWords.select(
    col("Title"),
    expr("Title_Words[0]"),
    size(col("Title_Words")),
    array_contains(col("Title_Words"), "Love")
  ).show()


}
