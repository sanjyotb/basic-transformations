package thoughtworks

import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import thoughtworks.AnalyzerUtils._

object Analyzer {

  import org.apache.spark.sql.functions._

  implicit class DiamondsDataframe(val diamondsDF: Dataset[Row]) {

    //Total records in diamonds
    def totalQuantity(spark: SparkSession): Long = {
      diamondsDF.countRows(spark)
    }

    //Average price of all diamonds
    def averagePrice(spark: SparkSession): Double = {
      diamondsDF.averageOfAColumn(spark, "price")
    }

    //Minimum price of all diamonds
    def minimumPrice(spark: SparkSession): Double = {
      diamondsDF.minimumOfAColumn(spark, "price")
    }

    //Maximum price of all diamonds
    def maximumPrice(spark: SparkSession): Double = {
      diamondsDF.maximumOfAColumn(spark, "price")
    }

    //Filter premium cut diamonds and fetch record count
    def totalPremiumCutDiamonds(spark: SparkSession): Long = {
      diamondsDF.filterAColumn(spark, diamondsDF("cut") rlike s"(?i)premium").count()
    }

    //Evaluate average price of diamonds by clarity using groupby and avg functions
    def averagePriceByClarity(spark: SparkSession): Dataset[Row] = {
      diamondsDF.groupBy("clarity").avg("price")
      //diamondsDF.select("clarity","price").groupBy(diamondsDF("clarity")).avg("price")
    }

    def dropColorColumn(spark: SparkSession): Dataset[Row] = {
      diamondsDF.drop(("color"))
    }

    //Drop id column and then check for duplicates
    def removeDuplicateRecords(spark: SparkSession): Dataset[Row] = {
      diamondsDF.drop("_c0").dropDuplicates()
    }

    private def checkColumnForValue(colName: String, regex: String): Column = col(colName) rlike regex

    //Populate column grade based on cut and clarity using when - otherwise conditionals
    def computeGrade(spark: SparkSession): Dataset[Row] = {
      diamondsDF
        .withColumn("grade",
          when(checkColumnForValue("cut", "((?i)(premium)|(ideal))")
            and checkColumnForValue("clarity", "((?i)(FL)|(IF)|(VVS1)|(VVS2))"), "A")
            .when(checkColumnForValue("cut", "((?i)(very good)|(good))")
              and checkColumnForValue("clarity", "((?i)(VS1)|(VS2)|(SI1)|(SI2))"), "B")
            .otherwise("C"))
    }

    def isGradeA(spark: SparkSession): Column = {
      import spark.implicits._

      (
        (lower($"cut") === lit("premium") ||
          lower($"cut") === lit("ideal"))
          &&
          (lower($"clarity") === lit("fl") ||
            lower($"clarity") === lit("if") ||
            lower($"clarity") === lit("vvs1") ||
            lower($"clarity") === lit("vvs2"))
        )
    }

    def isGradeB(spark: SparkSession): Column = {
      import spark.implicits._

      (
        (lower($"cut") === lit("very good") ||
          lower($"cut") === lit("good"))
          &&
          (lower($"clarity") === lit("vs1") ||
            lower($"clarity") === lit("vs2") ||
            lower($"clarity") === lit("si1") ||
            lower($"clarity") === lit("si2"))
        )
    }
  }

}
