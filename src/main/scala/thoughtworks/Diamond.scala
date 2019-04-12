package thoughtworks


import org.apache.spark.sql.SparkSession

object Diamond {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Analyze Diamond Data Spark App").getOrCreate()

    val diamondDF = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .csv("./src/main/resources/diamonds.csv")
      .toDF("id", "carat", "cut", "color", "clarity", "depth_percent", "table", "price", "length", "width", "depth")
      .cache()

    import thoughtworks.Analyzer._

    val totalNumRows = diamondDF.countRows(spark)
    val averagePrice = diamondDF.averageOfAColumn(spark, "price")

    println("Initial Analysis of Diamonds Data shows: \n")
    println(s"The schema of the data is")
    diamondDF.printSchema()
    println(s"The total number of rows is $totalNumRows")
    println(f"The average price of diamonds sold is $averagePrice%1.2f")

  }

}
