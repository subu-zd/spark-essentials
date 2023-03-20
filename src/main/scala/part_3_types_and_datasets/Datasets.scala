package part_3_types_and_datasets

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.{Dataset, Encoders, SparkSession}

import java.sql.Date

object Datasets extends App {

  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()

  val numbersDF = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/data/numbers.csv")

  numbersDF.printSchema()

  // returns an encoder of int
  // it has the capability of turning a row of a dataframe into an int
  // this helps us convert a DF to a Dataset
  // the below method works for dataframes with single columns and simple data types
  implicit val intEncoder = Encoders.scalaInt
  val numbersDS: Dataset[Int] = numbersDF.as[Int]

  numbersDS.filter(_ < 100)

  // dataset of a complex type
  // step 1 - define your case class
  case class Car(
    Name: String,
    Miles_per_Gallon: Option[Double],
    Cylinders: Long,
    Displacement: Double,
    Horsepower: Option[Long],
    Weight_in_lbs: Long,
    Acceleration: Double,
    Year: Date,
    Origin: String)

  // step 2 - read the DF from the file
  def readDF(fileName: String, fileType: String) = spark.read.format(fileType)
    .option("inferSchema", "true")
    .json(s"src/main/resources/data/$fileName")

  val carsDF = readDF("cars.json", "json")
    .withColumn("Year", col("Year").cast(DateType))
  // implicit val carEncoder = Encoders.product[Car]
  // Encoders.product takes any type parameter which extends the "product" type.
  // by default all case classes extend the product type
  // this helps spark to identify which fields map to which column in the dataframe

  // in real life however, writing an encoder for each case class for each dataset is NOT efficient
  // hence, all such implicits are wrapped in the spark.implicits lib by default

  // step 3 - defining an encoder
  import spark.implicits._

  // convert DF to DS
  val carsDS = carsDF.as[Car]

  numbersDS.filter(_ < 100).show()

  // map, flatMap, fold, reduce, for comprehensions
  val carNameDs = carsDS.map(_.Name.toUpperCase())
  carNameDs.show
  // in order to allow nulls for a dataset, use Option[T] when defining schema

  /*
  * Exercise
  * 1. Count how many cars we have
  * 2. Count how many POWERFUL cars we have (HP > 140)
  * 3. Average HP for the entire dataset */

  // 1
  val totalCars = carsDS.count()
  println(totalCars)

  // 2
  println(carsDS.filter(_.Horsepower.getOrElse(0L) > 140L).count())

  // 3
  // val carsHP = carsDS.map(_.Horsepower.getOrElse(0L)).collect()
  val carsHP = carsDS.map(_.Horsepower.getOrElse(0L)).reduce(_ + _)
  println(carsHP / totalCars)

  carsDS.select(avg(col("Horsepower"))).show()
  /*
  * Datasets have access to all the Dataframe functions because
  * Dataframes are essentially Dataset[Row]
  * */

  // joins
  case class Guitar(id: Long, make: String, model: String, `type`: String)
  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)
  case class Band(id: Long, name: String, hometown: String, year: Long)

  val guitarsDS = readDF("guitars.json", "json").as[Guitar]
  val guitarPlayersDS = readDF("guitarPlayers.json", "json").as[GuitarPlayer]
  val bandDS = readDF("bands.json", "json").as[Band]

  // calling a "join" on a DS returns a DF, hence use "joinWith" to preserve the DS type
  val guitarPlayerBandsDS = guitarPlayersDS.joinWith(bandDS, guitarPlayersDS.col("band") === bandDS.col("id"), "inner")
  guitarPlayerBandsDS.show()

  /*
  * Exercise: join the guitarsDS and guitarPlayerDS, in an outer join
  * hint - array_contains */
  val guitarGuitarPlayersDF: Dataset[(GuitarPlayer, Guitar)] = guitarPlayersDS.joinWith(guitarsDS, array_contains(guitarPlayersDS.col("guitars"), guitarsDS.col("id")), "outer")
  guitarGuitarPlayersDF.show()

  // Grouping
  val carsGroupedByOrigin = carsDS.groupByKey(_.Origin).count()
  carsGroupedByOrigin.show()
  // joins and groups are WIDE transformations, will involve SHUFFLE operations
}
