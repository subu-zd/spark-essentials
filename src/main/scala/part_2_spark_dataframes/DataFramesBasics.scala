package part_2_spark_dataframes

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

object DataFramesBasics extends App {

  val spark = SparkSession.builder()
    .appName("DataFrames Basics")
    .config("spark.master", "local") // the config for spark.master is local as we're executing spark applications locally on the computer
    .getOrCreate() // this will create a new Spark Session

  // reading a dataframe
  val firstDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars.json")

  firstDF.show()
  firstDF.printSchema()

  /* What is a DataFrame?
  it is essentially the schema (which is the description of the attributes of the data)
    root
    |-- Acceleration: double (nullable = true)
    |-- Cylinders: long (nullable = true)
    |-- Displacement: double (nullable = true)
    |-- Horsepower: long (nullable = true)
    |-- Miles_per_Gallon: double (nullable = true)
    |-- Name: string (nullable = true)
    |-- Origin: string (nullable = true)
    |-- Weight_in_lbs: long (nullable = true)
    |-- Year: string (nullable = true)

  and a distributed collection of rows that are conforming to that schema.

   +------------+---------+------------+----------+----------------+--------------------+------+-------------+----------+
  |Acceleration|Cylinders|Displacement|Horsepower|Miles_per_Gallon|                Name|Origin|Weight_in_lbs|      Year|
  +------------+---------+------------+----------+----------------+--------------------+------+-------------+----------+
  |        12.0|        8|       307.0|       130|            18.0|chevrolet chevell...|   USA|         3504|1970-01-01|
  |        11.5|        8|       350.0|       165|            15.0|   buick skylark 320|   USA|         3693|1970-01-01|

  etc ...
  * */

  firstDF.take(5).foreach(println)
  /* prints an array of rows while conforming to the DF schema

    [12.0,8,307.0,130,18.0,chevrolet chevelle malibu,USA,3504,1970-01-01]
    [11.5,8,350.0,165,15.0,buick skylark 320,USA,3693,1970-01-01]
    [11.0,8,318.0,150,18.0,plymouth satellite,USA,3436,1970-01-01]
    [12.0,8,304.0,150,16.0,amc rebel sst,USA,3433,1970-01-01]
    [10.5,8,302.0,140,17.0,ford torino,USA,3449,1970-01-01]
  * */

  // spark type - these types are stored internally as case classes. These are known an runtime and NOT compile time when Spark evaluates the data

  val longType = LongType // singleton

  // schema
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

  // creating schema by yourself is the best practice instead of using the "inferSchema" config

  val carsDFSchema = firstDF.schema
  println(carsDFSchema)

  // read DF with schema
  val carsDFWithSchema = spark.read
    .format("json")
    .schema(carsDFSchema)
    .load("src/main/resources/data/cars.json")

  // create rows by hand
  val myRow = Row("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA")

  // create DF from tuples
  val cars = Seq(
    ("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA"),
    ("buick skylark 320",15.0,8L,350.0,165L,3693L,11.5,"1970-01-01","USA"),
    ("plymouth satellite",18.0,8L,318.0,150L,3436L,11.0,"1970-01-01","USA"),
    ("amc rebel sst",16.0,8L,304.0,150L,3433L,12.0,"1970-01-01","USA"),
    ("ford torino",17.0,8L,302.0,140L,3449L,10.5,"1970-01-01","USA"),
    ("ford galaxie 500",15.0,8L,429.0,198L,4341L,10.0,"1970-01-01","USA"),
    ("chevrolet impala",14.0,8L,454.0,220L,4354L,9.0,"1970-01-01","USA"),
    ("plymouth fury iii",14.0,8L,440.0,215L,4312L,8.5,"1970-01-01","USA"),
    ("pontiac catalina",14.0,8L,455.0,225L,4425L,10.0,"1970-01-01","USA"),
    ("amc ambassador dpl",15.0,8L,390.0,190L,3850L,8.5,"1970-01-01","USA")
  )

  val manualCarsDF = spark.createDataFrame(cars) // since the types of values in the tuples are known at compile time,
  // Spark automatically creates a schema for you. But it doesn't assign any column names

  // NOTE : Schemas are only applicable to DataFrames and not rows (there is no structure imposed on rows)

  // create DFs with implicits
  import spark.implicits._
  val manualCarsDFWithImplicits = cars.toDF("Name", "MPG", "Cylinders", "Displacement", "HP", "Weight", "Acceleration", "Year", "Country")

  manualCarsDF.printSchema()
  manualCarsDFWithImplicits.printSchema()

  /*
  * Exercises
  * 1) Create a manual DF describing smartphones
  *   - make
  *   - model
  *   - screen dimensions
  *   - camera megapixel
  *
  * 2) Read another files from the data/ folder
  * - print its schema
  * - Count the number of rows, call count()
  * */

  // 1)
  val phones = Seq(
    ("Apple", "iPhone 13", 6.1, 12),
    ("Samsung", "Galaxy S23 Ultra", 6.5, 48),
    ("OnePlus", "10 Pro", 6.5, 32)
  )

  val phonesManualDF = phones.toDF("Make", "Model", "ScreenSize", "Megapixels")
  phonesManualDF.printSchema()
  phonesManualDF.show()

  // 2)
  val moviesDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")

  moviesDF.printSchema()
  println(moviesDF.count())
}
