package thoughtworks

import org.apache.spark.sql.functions.{lit, lower}
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import thoughtworks.AnalyzerUtils._

object Analyzer {

  implicit class DiamondsDataframe(val diamondsDF: Dataset[Row]) {

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

    def totalPremiumCutDiamonds(spark: SparkSession): Long = {
      import spark.implicits._

      val isPremiumCut = lower($"cut") === lit("premium")
      diamondsDF
        .filterAColumn(spark, isPremiumCut)
        .countRows(spark)
    }

    def averagePriceByClarity(spark: SparkSession): Dataset[Row] = {
      import org.apache.spark.sql.functions._

      diamondsDF
        .groupBy("clarity")
        .agg(
          round(avg("price"), 2).alias("averagePrice")
        )
    }

    def dropColorColumn(spark: SparkSession): Dataset[Row] = {
      diamondsDF.dropAColumn(spark, "color")
    }

    def removeDuplicateRecords(spark: SparkSession): Dataset[Row] = {
      diamondsDF
        .dropAColumn(spark, "id")
        .dropDuplicateRecords(spark)
    }

    def computeGrade(spark: SparkSession): Dataset[Row] = {
      import org.apache.spark.sql.functions._

      val columnValue = when(isGradeA(spark), "A")
        .otherwise(
          when(isGradeB(spark), "B")
            .otherwise("C")
        )
      diamondsDF.addAColumn(spark, "grade", columnValue)
    }

    def isGradeA(spark: SparkSession):Column = {
      import spark.implicits._

      (
        (lower($"cut") === lit("premium") ||
        lower($"cut") === lit("ideal"))
      &&
        (lower($"clarity") === lit("fl")  ||
        lower($"clarity") === lit("if")   ||
        lower($"clarity") === lit("vvs1") ||
        lower($"clarity") === lit("vvs2"))
      )
    }

    def isGradeB(spark: SparkSession):Column = {
      import spark.implicits._

      (
        (lower($"cut") === lit("very good") ||
        lower($"cut") === lit("good"))
      &&
        (lower($"clarity") === lit("vs1") ||
        lower($"clarity") === lit("vs2")  ||
        lower($"clarity") === lit("si1")  ||
        lower($"clarity") === lit("si2"))
      )
    }
  }
}
