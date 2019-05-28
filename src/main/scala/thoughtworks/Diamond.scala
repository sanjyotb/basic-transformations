package thoughtworks

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import thoughtworks.Analyzer._

object Diamond {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("Analyze Diamond Data Spark App").getOrCreate()

    //Create dataframe from src/main/resources/diamonds.csv
    val diamondDF: DataFrame = spark
      .read
      .option("inferSchema", true)
      .option("header", true)
      .csv("./src/main/resources/diamonds.csv")
    diamondDF.cache()

    val totalNumRows = diamondDF.totalQuantity(spark)
    //Perform operations on diamonds Dataset
    val diamondsWithoutDuplicates = diamondDF.removeDuplicateRecords(spark)
    val averagePrice = diamondDF.averagePrice(spark)
    val minPrice = diamondDF.minimumPrice(spark)
    val maxPrice = diamondDF.maximumPrice(spark)
    val filterFlawlessDiamonds = diamondDF.filter(!(col("clarity") rlike ("(?i)(FL)")))
    val groupByClarityAndCalcAvg = diamondDF.groupBy("clarity").agg(avg("price"))
    val withGradeColumn = diamondDF.computeGrade(spark)
    val dropColorColumn = diamondDF.dropColorColumn(spark)

    println("Initial Analysis of Diamonds Data shows: \n")
    println(s"The schema of the data is")
    diamondDF.printSchema()
    println(s"The total number of diamonds we have data about is $totalNumRows")
    //Print results of the operations performed
    println(s"Data after removing duplicates : ${diamondsWithoutDuplicates.show(5)}")
    println(s"Average price : $averagePrice")
    println(s"Minimum price : $minPrice")
    println(s"Maximum price : $maxPrice")
    println(s"Filter flawless diamonds : ${filterFlawlessDiamonds.show(5)}")
    println(s"Group by clarity and average : ${groupByClarityAndCalcAvg.show(20)}")
    println(s"Data with grade column : ${withGradeColumn.show(5)}")
    println(s"Data without color column : ${dropColorColumn.show(5)}")

  }
}
