package part_2_spark_dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr, when}

object ColumnsAndExpressions extends App {

  val spark = SparkSession.builder()
    .appName("DF Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  carsDF.show()

  // Columns
  val firstColumn = carsDF.col("Name")
  // the result of this is a Column object which will be used in Select expressions

  // Selecting (Projecting)
  val carNamesDF = carsDF.select(firstColumn)

  carNamesDF.show()

  // various select methods
  import spark.implicits._

  carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    'Year, // Scala symbol, auto-converted to column
    $"Horsepower", // fancier interpolated string, returns a Column object
    expr("Origin")
  )

  // select with plain column names
  val a = carsDF.select("Name", "Year")

  // selecting col names is the simplest version of EXPRESSIONS
  val simplestExpression = carsDF.col("Weight_in_lbs") // this returns a Column object which is a subtype of an EXPRESSION
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2 // the result of this expression is still a Column object
  // but this describes a transformation this time around as every val in the col will be divided by 2.2

  val carsWithWeightsDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kgs"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kgs_2")
  )

  carsWithWeightsDF.show()

  // selectExpr
  val carsWithSelectExprWeightsDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )

  // DF Processing

  // adding a column
  val carsWithKg3DF = carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)

  // renaming a column
  val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight in pounds")

  // careful with column names - use backticks to escape reserved characters like spaces or hyphens
  carsWithColumnRenamed.selectExpr("`Weight in pounds`")

  // remove a column
  carsWithColumnRenamed.drop("Cylinders", "Displacement")

  // filtering
  val europeanCarsDF = carsDF.filter(col("Origin") =!= "USA")
  val europeanCarsDF2 = carsDF.where(col("Origin") =!= "USA")

  // filtering with expression strings
  val americanCarsDF = carsDF.filter("Origin = 'USA'")

  // chain filters
  val americanPowerfulCarsDF = carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)
  val americanPowerfulCarsDF2 = carsDF.filter(col("Origin") === "USA" and col("Horsepower") > 150)
  val americanPowerfulCarsDF3 = carsDF.filter("Origin = 'USA' and Horsepower > 150")

  // unioning = adding more rows
  val moreCarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/more_cars.json")

  val allCarsDF = carsDF.union(moreCarsDF)

  // distinct values
  val allCountriesDF = carsDF.select("Origin").distinct()
  allCountriesDF.show()

  // EXERCISES
  /*
  * 1. Read the movies DF and select 2 columns of your choice
  * 2. Create new col by summing up all the gross profits of the movie
  * 3. Select all the comedy movies with IMDb > 6
  *
  * Use as many versions as possible
  * */

  // 1)

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  val moviesDF2Cols = moviesDF.selectExpr(
    "Title",
    "Distributor"
  )

  moviesDF2Cols.show()

  // 2)
  val moviesWithTotalProfitDF = moviesDF.withColumn("Total_Profit", $"US_Gross" + $"Worldwide_Gross" + when(col("US_DVD_Sales").isNull, 0).otherwise(col("US_DVD_Sales")))
  moviesWithTotalProfitDF.show(10)

  // 3)
  val funnyComedyMoviesDF = moviesDF.select("Title", "IMDB_Rating")
    .where("Major_Genre = 'Comedy' and IMDB_Rating > 6")

  funnyComedyMoviesDF.show(10)
}
