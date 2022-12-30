package part_2_spark_dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Joins extends App{
  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")

  val guitaristsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")

  // joins
  val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")
  val guitaristsBandsDF = guitaristsDF.join(bandsDF, joinCondition, joinType = "inner")

  // inner join is the default value
  guitaristsBandsDF.show()

  // outer joins
  guitaristsDF.join(bandsDF, joinCondition, "left_outer").show()
  // left outer -> everything in the inner join + all the rows in the LEFT table with NULLS where data is missing

  guitaristsDF.join(bandsDF, joinCondition, "right_outer").show()
  // right outer -> everything in the inner join + all the rows in the RIGHT table with NULLS where the data is missing

  guitaristsDF.join(bandsDF, joinCondition, "full_outer").show()
  // full_outer -> everything in the inner join + all the row in BOTH tables with NULLS where the data is missing

  // semi joins
  guitaristsDF.join(bandsDF, joinCondition, "left_semi").show()
  // left_semi -> left_outer - (minus) inner join and only rows from the left table
  // everything in the LEFT data frame for which there is A ROW in the RIGHT data frame satisfying the condition

  // anti joins
  guitaristsDF.join(bandsDF, joinCondition, "left_anti").show()
  // only keeps the data from the LEFT data frame for which there is NO ROW in the RIGHT data from satisfying the join condition

  // things to bear in mind

  // guitaristsBandsDF.select("id", "band").show() -> this will crash as joins might lead to col name duplication
  // and hence, an ambiguity when referencing the said name causing spark to crash

  // option 1 - rename the column on which we are joining
  guitaristsDF.join(bandsDF.withColumnRenamed("id", "band"), "band")

  // option 2 - drop the dupe column
  guitaristsBandsDF.drop(bandsDF.col("id"))
  // spark maintains unique identifiers for each and every column it operates on
  // the above LOC - drops from the guitaristsBandsDF the column with the unique identifier obtained from bands
  // using bandsDF.col("id") attaches this unique identifier to the col

  // option 3 - rename the offending col and keep the data
  val bandsModDF = bandsDF.withColumnRenamed("id", "bandId")
  guitaristsDF.join(bandsModDF, guitaristsDF.col("id") === bandsModDF.col("bandId")) // still has duplicate data

  // using complex types
  guitaristsDF.join(guitarsDF.withColumnRenamed("id", "guitarId"), joinExprs = expr("array_contains(guitars, guitarId)"))

  // EXERCISES
  /*
  * - show all employees and their max salary
  * - show all employees who were never managers
  * - find the job titles of the best paid 10 employees in the company
  * */

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

  val employeesDF = readTable("employees")
  val salariesDF = readTable("salaries")
  val departmentManagersDF = readTable("dept_manager")
  val titlesDF = readTable("titles")

  // 1)
  // select e.*, MAX(s.salary) from employees e inner join salaries s on e.emp_no = s.emp_no group by e.emp_no;
  val maxSalaryTemp = salariesDF.groupBy("emp_no").agg(max("salary").as("maxSalary"))
  val maxSalariesPerEmpNoDF = employeesDF.join(maxSalaryTemp, "emp_no")
  maxSalariesPerEmpNoDF.show()

  // 2)
  val nonManagerEmployees = employeesDF.join(departmentManagersDF, employeesDF.col("emp_no") === departmentManagersDF.col("emp_no"), "left_anti")
  nonManagerEmployees.show()

  // 3)
  // select distinct(e.emp_no), e.first_name, e.last_name, t.title, MAX(t.from_date), MAX(s.salary) as max_salary from employees e inner join salaries s on e.emp_no = s.emp_no inner join titles t on e.emp_no = t.emp_no group by e.emp_no, t.title order by max_salary desc limit 10;
  val mostRecentTitlePerEmpDF = titlesDF.groupBy("emp_no", "title").agg(max("to_date")) // .max() alone doesn't work with non-numeric columns
  val bestPaidEmployeesDF = maxSalariesPerEmpNoDF.orderBy(col("maxSalary").desc).limit(10)
  val bestPaidJobs =  bestPaidEmployeesDF.join(mostRecentTitlePerEmpDF, "emp_no")
  bestPaidJobs.show()
}
