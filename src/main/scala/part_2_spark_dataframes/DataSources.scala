package part_2_spark_dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._

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
    StructField("Year", DateType),
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

  // JSON flags
  spark.read
    .schema(carsSchema)
    .option("dateFormat", "YYYY-MM-dd") // only works with enforced schema
    // if we don't specify the date format for DateType, spark will try to parse it using the standard ISO format
    // NOTE: if there is a mismatch between the date value and format, spark will fail parsing and put in a NULL for that cell
    // timestampFormat can be used for precision upto seconds
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed") // uncompressed (default), bzip2, gzip, lz4, snappy, deflate
    .json("src/main/resources/data/cars.json")

  // CSV flags
  val stocksSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  ))

  // the names of columns in the schema should match the names in the CSV files

  spark.read
    .schema(stocksSchema)
    .option("dateFormat", "MMM dd YYYY")
    .option("header", "true")
    .option("sep", ",") // semicolon, tab etc.
    .option("nullValue", "")
    .csv("src/main/resources/data/stocks.csv")

  // Parquet
  // It is an Open source compressed binary data storage format optimized for fast reading of columns
  // works very well with Spark
  // default storage format for data frames in spark

  // one adv is that it is very predictable so we don't need many options like CSV.
  // Just mentioning that the format is Parquet will ensure that the file is stored in the right format

  carsDF.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars.parquet")
  // since parquet is the default format, we don't need to explicitly specify the format.
  // 6-10 times compression v/s json

  // Text files
  spark.read.text("src/main/resources/data/sampleTextFile.txt").show()

  // Reading from a remote DB

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  val employeesDF = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url",url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.employees")
    .load()

  employeesDF.show()

  /*
  * Exercise: read the movies DF, then write it as
  * - tab-separated values file
  * - snappy parquet
  * - table "public.movies" in the Postgres DB
  * */

//  val moviesSchema = StructType(Array(
//    StructField("Title", StringType),
//    StructField("US_Gross", IntegerType),
//    StructField("Worldwide_Gross", IntegerType),
//    StructField("US_DVD_Sales", IntegerType),
//    StructField("Production_Budget", IntegerType),
//    StructField("Release_Date", DateType),
//    StructField("MPAA_Rating", StringType),
//    StructField("Running_Time_min", IntegerType),
//    StructField("Distributor", StringType),
//    StructField("Source", StringType),
//    StructField("Major_Genre", StringType),
//    StructField("Creative_Type", StringType),
//    StructField("Director", StringType),
//    StructField("Rotten_Tomatoes_Rating", IntegerType),
//    StructField("IMDB_Rating", DoubleType),
//    StructField("IMDB_Votes", IntegerType)
//  ))

  val moviesDF = spark.read
    .json("src/main/resources/data/movies.json")

  moviesDF.write
    .option("header", "true")
    .option("sep", "\t")
    .mode(SaveMode.Overwrite)
    .csv("src/main/resources/data/movies.csv")


  moviesDF.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/movies.parquet")

  moviesDF.write
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", "public.movies")
    .save()
}
