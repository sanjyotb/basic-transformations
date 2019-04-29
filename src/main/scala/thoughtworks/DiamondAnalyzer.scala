package thoughtworks

import org.apache.spark.sql.functions.{avg, lit, lower, min}
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}

object DiamondAnalyzer {

  implicit class DiamondsDataframe(val diamondsDF: Dataset[Row]) {

    def totalQuantity(spark: SparkSession): Long = {
      diamondsDF.count()
    }

    def averagePrice(spark: SparkSession): Double = {
      import spark.implicits._

      val dataset: Dataset[Double] = diamondsDF.select(avg("price")).as[Double]

      dataset.collect()(0)
    }

    def minimumPrice(spark: SparkSession): Double = {
      import spark.implicits._

      val dataset: Dataset[Double] = diamondsDF.select(min("price")).as[Double]

      dataset.collect()(0)
    }

    def maximumPrice(spark: SparkSession): Double = {
      import spark.implicits._
      import org.apache.spark.sql.functions.max

      val dataset: Dataset[Double] = diamondsDF.select(max("price")).as[Double]

      dataset.collect()(0)
    }

    def totalPremiumCutDiamonds(spark: SparkSession): Long = {
      import spark.implicits._

      diamondsDF
        .filter(lower($"cut") === lit("premium"))
        .count()
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
      diamondsDF.drop("color")
    }

    def removeDuplicateRecords(spark: SparkSession): Dataset[Row] = {
      diamondsDF
        .drop("id")
        .dropDuplicates()
    }

    def computeGrade(spark: SparkSession): Dataset[Row] = {
      import org.apache.spark.sql.functions._

      val columnValue = when(isGradeA(spark), "A")
        .otherwise(
          when(isGradeB(spark), "B")
            .otherwise("C")
        )

      diamondsDF.withColumn("grade", columnValue)
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
