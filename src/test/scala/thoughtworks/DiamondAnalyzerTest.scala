package thoughtworks

import org.scalatest.{Ignore, Matchers}
import thoughtworks.DiamondAnalyzer._

@Ignore
class DiamondAnalyzerTest extends FeatureSpecWithSpark with Matchers {

  import spark.implicits._

  feature("count of records in diamonds dataset") {
    scenario("calculate total quantity of diamonds") {
      val columnName = "index"
      val testDF = Seq("diamond-1", "diamond-2", "diamond-3").toDF(columnName)

      val totalNumberOfDiamonds = testDF.totalQuantity(spark)

      totalNumberOfDiamonds should be(3)
    }
  }

  feature("average price of all diamonds") {
    scenario("calculate the average price of all diamonds") {
      val columnName = "price"
      val testDF = Seq(1.0, 2.0, 3.0).toDF(columnName)

      val averagePriceOfDiamonds = testDF.averagePrice(spark)

      averagePriceOfDiamonds should be(2.0)
    }
  }

  feature("minimum price of diamonds") {
    scenario("calculate the minimum price of all diamonds") {
      val columnName = "price"
      val testDF = Seq(1.0, 2.0, 3.0).toDF(columnName)

      val minPriceOfDiamonds = testDF.minimumPrice(spark)

      minPriceOfDiamonds should be(1.0)
    }
  }

  feature("maximum price of diamonds") {
    scenario("calculate the maximum price of all diamonds") {
      val columnName = "price"
      val testDF = Seq(1.0, 2.0, 3.0).toDF(columnName)

      val maxPriceOfDiamonds = testDF.maximumPrice(spark)

      maxPriceOfDiamonds should be(3.0)
    }
  }

  feature("filter premium cut diamonds") {
    scenario("calculate the total number of premium cut diamonds") {
      val columnName = "cut"
      val testDF = Seq("premium", "Good", "Premium").toDF(columnName)

      val totalPremiumCutDiamonds = testDF.totalPremiumCutDiamonds(spark)

      totalPremiumCutDiamonds should be(2)
    }
  }

  feature("groupby clarity and calculate average price") {
    scenario("calculate average price for various clarities of diamond") {
      val clarityCol = "clarity"
      val priceCol = "price"

      val testDF = Seq(("FL",1.0), ("IF", 2.0), ("VVS1",3.0), ("IF",4.0)).toDF(clarityCol, priceCol)

      val actualAvgPriceByClarityDF = testDF.averagePriceByClarity(spark)

      val expectedAvgPriceByClarityDF = Seq(("FL",1.0), ("IF", 3.0), ("VVS1",3.0)).toDF(clarityCol, priceCol)

      actualAvgPriceByClarityDF.except(expectedAvgPriceByClarityDF).count() should be(0)
      expectedAvgPriceByClarityDF.except(actualAvgPriceByClarityDF).count() should be(0)
    }
  }

  feature("compute grade for all diamonds based on the cut and clarity") {

    scenario("grade should be 'A' if cut is premium or ideal " +
      "and clarity is FL, IF, VVS1 or VVS2") {
      val clarityCol = "clarity"
      val cutCol = "cut"
      val gradeCol = "grade"

      val testDF = Seq(("FL","premium"), ("IF", "ideal"), ("VVS1","premium"), ("VVS2","ideal"))
        .toDF(clarityCol, cutCol)

      val actualDF = testDF.computeGrade(spark)

      val expectedDF = Seq(("FL","premium", "A"), ("IF", "ideal","A"), ("VVS1","premium","A"), ("VVS2","ideal","A"))
        .toDF(clarityCol, cutCol, gradeCol)

      actualDF.except(expectedDF).count() should be(0)
      expectedDF.except(actualDF).count() should be(0)
    }

    scenario("grade should be 'B' if cut is very good or good " +
      "and clarity is VS1, VS2, SI1 or SI2") {
      val clarityCol = "clarity"
      val cutCol = "cut"
      val gradeCol = "grade"

      val testDF = Seq(("VS1","very good"), ("VS2", "good"), ("SI1","Very Good"), ("SI2","good"))
        .toDF(clarityCol, cutCol)

      val actualDF = testDF.computeGrade(spark)

      val expectedDF = Seq(("VS1","very good", "B"), ("VS2", "good","B"), ("SI1","Very Good","B"), ("SI2","good","B"))
        .toDF(clarityCol, cutCol, gradeCol)

      actualDF.except(expectedDF).count() should be(0)
      expectedDF.except(actualDF).count() should be(0)
    }

    scenario("grade should be 'C' if conditions for grade A or B are not satisfied") {
      val clarityCol = "clarity"
      val cutCol = "cut"
      val gradeCol = "grade"

      val testDF = Seq(("I1","very good"), ("I2", "good"), ("SI2","fair"), ("I3", "good"))
        .toDF(clarityCol, cutCol)

      val actualDF = testDF.computeGrade(spark)
      val expectedDF = Seq(("I1","very good", "C"), ("I2", "good","C"), ("SI2","fair","C"),  ("I3", "good","C"))
        .toDF(clarityCol, cutCol, gradeCol)

      actualDF.except(expectedDF).count() should be(0)
      expectedDF.except(actualDF).count() should be(0)
    }
  }

  feature("Purge duplicate diamonds in the dataset") {
    scenario("drop duplicate records of diamonds") {
      val clarityCol = "clarity"
      val cutCol = "cut"

      val testDF = Seq(("I1","very good"), ("I2", "fair"), ("I2","fair"), ("I3", "good"))
        .toDF(clarityCol, cutCol)

      val actualDF = testDF.removeDuplicateRecords(spark)

      val expectedDF = Seq(("I1","very good"), ("I2","fair"), ("I3", "good"))
        .toDF(clarityCol, cutCol)

      actualDF.except(expectedDF).count() should be(0)
      expectedDF.except(actualDF).count() should be(0)
    }
  }

  feature("remove color data from diamonds dataset") {
    scenario("drop column color from diamonds dataset") {
      val colorCol = "color"
      val cutCol = "cut"

      val testDF = Seq(("D", "very good"), ("E", "fair"), ("F", "fair"), ("J", "good"))
        .toDF(colorCol, cutCol)

      val actualDF = testDF.dropColorColumn(spark)

      val expectedDF = Seq("very good", "fair", "good")
        .toDF(cutCol)

      actualDF.except(expectedDF).count() should be(0)
      expectedDF.except(actualDF).count() should be(0)
    }
  }
}
