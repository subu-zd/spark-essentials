package part_4_spark_sql

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.col

object SparkSql extends App {

  val spark = SparkSession.builder()
    .appName("Spark SQL practice")
    .config("spark.master", "local")
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // regular DF API
  carsDF.select(col("Name")).where(col("Origin") === "USA")


  // use spark SQL
  carsDF.createOrReplaceTempView("cars")
  val americanCarsDF = spark.sql(
    """
      |select Name from cars where Origin = 'USA'
      |""".stripMargin
  )

  americanCarsDF.show()

  // we can run any SQL statement
  spark.sql("create database subz")
  spark.sql("use subz")
  spark.sql("show tables from subz").show()


  // transfer tables from a DB to Spark tables

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  def readTable(tableName: String) = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", s"public.$tableName")
    .load()

  def transferTables(tableNames: List[String]) = tableNames.foreach { tableName =>
    val tableDF = readTable(tableName)
    tableDF.createOrReplaceTempView(tableName)
//    tableDF.write
//      .mode(SaveMode.Overwrite)
//      .saveAsTable(tableName)
  }

  transferTables(List(
    "employees",
    "departments",
    "dept_manager",
    "dept_emp",
    "titles",
    "salaries"
  ))

//  val employeesDF = readTable("employees")
//  employeesDF.write
//    .mode(SaveMode.Overwrite)
//    .saveAsTable("employees")

  // read a dataframe from a spark table in a data warehouse

  val employeesDF2 = spark.read.table("employees")
  employeesDF2.show(5)
}
