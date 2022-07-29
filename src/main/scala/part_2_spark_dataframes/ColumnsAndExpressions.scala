package part_2_spark_dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

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
  carsDF.select("Name", "Year")

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
    "Name"
  )
}
