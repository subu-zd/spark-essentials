package part_3_types_and_datasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.annotation.tailrec

object CommonTypes extends App {

  val spark = SparkSession.builder()
    .appName("Common spark types")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // adding a plain value to a DF
  moviesDF.select(col("Title"), lit(47).as("plain_value")).show()

  // Booleans
  val dramaFilter = col("Major_Genre") equalTo "Drama"
  val ratingFilter = col("IMDB_Rating") > 7.0
  val preferredFilter = dramaFilter and ratingFilter

  moviesDF.select("Title").where(preferredFilter)
  // multiple ways of filtering

  val moviesWithGoodnessFlagDF = moviesDF.select(col("Title"), preferredFilter.as("good_movie")) // boolean column in the resulting DF

  moviesWithGoodnessFlagDF.show()
  moviesWithGoodnessFlagDF.where("good_movie").show() // where(col("good_movie") === true)

  // Numbers

  // math operators
  val moviesAvgRatingsDF = moviesDF.select(col("Title"), (col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating")) / 2)

  // correlation = number between -1 and 1
  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating")) // corr is an Action (unlike a transformation, it is NOT lazy)

  // Strings

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // capitalisation: initcap, lower, upper
  carsDF.select(initcap(col("Name"))).show()

  // contains
  carsDF.select("*").where(col("Name").contains("volkswagen"))

  // regex
  val regexString = "volkswagen|vw"
  val vwDF = carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), regexString, 0).as("regex_extract")
  ).where(col("regex_extract") =!= "").drop("regex_extract")

  vwDF.show()

  vwDF.select(
    col("Name"),
    regexp_replace(col("Name"), regexString, "People's car").as("regex_replace")
  ).show()

  /*
  * EXERCISE
  *
  * - filter the cars DF by a list of cars name obtained by an API
  * */

  def getCarNames: List[String] = List("Mercedes-Benz", "Volkswagen", "Ford")

//  def buildRegex(cars: List[String]): String = {
//    @tailrec
//    def helper(acc: String, carsAcc: List[String]): String = {
//      if (carsAcc.isEmpty) ""
//      if (carsAcc.length == 1) acc + carsAcc.head
//      else helper(acc + carsAcc.head + "|", carsAcc.tail)
//    }
//
//    helper("", cars.map(_.toLowerCase()))
//  }

  val complexRegex: String = getCarNames.map(_.toLowerCase()).mkString("|")

  // version 1 with regex
  val complexFilterDF = carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), complexRegex, 0).as("regex_extract")
  ).where(col("regex_extract") =!= "")
    .drop("regex_extract")
    .show()

  // version 2 with contains
  val carNameFilters = getCarNames.map(_.toLowerCase()).map(name => col("Name").contains(name))
  val bigFilter = carNameFilters.fold(lit(false))((combinedFilter, newCarNameFilter) => combinedFilter or newCarNameFilter)

  carsDF.filter(bigFilter).show()

}
