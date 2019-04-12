package thoughtworks

import org.apache.spark.sql.{ Dataset, Row, SparkSession}
import thoughtworks.AnalyzerUtils._

object Analyzer {

  implicit class DiamondsDataframe(val diamondsDF: Dataset[Row]) {

    def totalPremiumCutDiamonds(spark: SparkSession): Long = {
      import spark.implicits._
      import org.apache.spark.sql.functions._

      val isPremiumCut = lower($"cut") === lit("premium")
      diamondsDF
        .filterAColumn(spark, isPremiumCut)
        .countRows(spark)
    }

    def totalQuantity(spark: SparkSession): Long = {
      diamondsDF.countRows(spark)
    }

    def averagePrice(spark: SparkSession): Double = {
      diamondsDF.averageOfAColumn(spark, "price")
    }

    def minimumPrice(spark: SparkSession): Double = {
      diamondsDF.minimumOfAColumn(spark, "price")
    }

    def maximumPrice(spark: SparkSession): Double = {
      diamondsDF.maximumOfAColumn(spark, "price")
    }
  }
}
