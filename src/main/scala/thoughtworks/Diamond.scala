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

    val totalNumRows = diamondDF.totalQuantity(spark)
    //Perform operations on diamonds Dataset

    println("Initial Analysis of Diamonds Data shows: \n")
    println(s"The schema of the data is")
    diamondDF.printSchema()
    println(s"The total number of diamonds we have data about is $totalNumRows")
    //Print results of the operations performed
  }
}
