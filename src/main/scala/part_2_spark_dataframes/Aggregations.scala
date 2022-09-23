package part_2_spark_dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Aggregations extends App {
  val spark = SparkSession.builder()
    .appName("Aggregation and Grouping")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // counting
  val genresCountDF = moviesDF.select(count(col("Major_Genre")))
  genresCountDF.show()
  // moviesDF.selectExpr("count(Major_Genre)") - same as above

  moviesDF.select(count("*")).show()

  // counting distinct values
  moviesDF.select(countDistinct(col("Major_Genre"))).show()

  // approximate count
  moviesDF.select(approx_count_distinct(col("Major_Genre"))).show()

  // min and max
  val minRatingDF = moviesDF.select(min(col("IMDB_Rating")))
  // moviesDF.selectExpr("min(IMDB_Rating)")

  // sum
  moviesDF.select(sum(col("US_Gross")))
  // moviesDF.selectExpr("sum(US_Gross)")

  // average
  moviesDF.select(avg(col("Rotten_Tomatoes_Rating")))
  // moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating)")

  moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating"))
  ).show()

  // grouping
  val countByGenre = moviesDF
    .groupBy(col("Major_Genre")) // includes nulls
    .count()
  // select count(*) from moviesDF group by Major_Genre

  countByGenre.show()

  val avgRatingByGenre = moviesDF
    .groupBy(col("Major_Genre"))
    .avg("IMDB_Rating")

  val aggregationByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    )
    .orderBy(col("Avg_Rating"))

  aggregationByGenreDF.show()

  /*
  * 1. Sum up ALL the profits of ALL the movies
  * 2. Count how many distinct directors in the DF
  * 3. Show the mean and stddev of US_Gross revenue for the movies
  * 4. Avg IMDB Rating and Avg US Gross per director
  * */

  // 1)
  val moviesWithTotalProfitDF = moviesDF.withColumn("Total_Profit", col("US_Gross") + col("Worldwide_Gross") + when(col("US_DVD_Sales").isNull, 0).otherwise(col("US_DVD_Sales")))

  moviesWithTotalProfitDF.select(sum(col("Total_Profit"))).show()

  moviesDF
    .select((col("US_Gross") + col("Worldwide_Gross")).as("Total_Gross"))
    .select(sum(col("Total_Gross")))
    .show()

  moviesDF
    .select((col("US_Gross") + col("Worldwide_Gross") + when(col("US_DVD_Sales").isNull, 0).otherwise(col("US_DVD_Sales"))).as("Total_Gross"))
    .select(sum(col("Total_Gross")))
    .show()

  // 2)
  moviesDF.select(countDistinct(col("Director"))).show()

  // 3)
  moviesDF.select(
    mean(col("US_Gross")),
    stddev(col("US_Gross"))
  ).show()

  // 4)
  moviesDF
    .groupBy("Director")
    .agg(
      avg("IMDB_Rating").as("Avg_IMDB_Rating"),
      avg("US_Gross").as("Avg_US_Gross")
    ).orderBy("Avg_US_Gross")
    .show()

  // aggregations and groupings are wide transformations
  // i.e. one/more input partitions => one/more output partitions
  // during shuffle data is being moves in between different nodes in the spark cluster
  // and this is an expensive operation

  // so for fast data pipelines, be mindful of using these aggregations during the spark transformations
  // often (NOT ALWAYS) it is the best practice to use data aggregation at the end of the pipeline
}
