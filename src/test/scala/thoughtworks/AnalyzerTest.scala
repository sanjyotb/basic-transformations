package thoughtworks

import org.scalatest.{Matchers}
import Analyzer._

class AnalyzerTest extends FeatureSpecWithSpark with Matchers {
  import spark.implicits._

  feature("calculate total number of rows") {
    scenario("count the number of rows in a dataframe") {
      val testDF = Seq("row1", "row2").toDF()

      val count: Long = testDF.countRows(spark)

      count should be(2)
    }
  }

  feature("calculate average of a column in dataframe") {
    scenario("calculate the average of a column with three items in a dataframe") {
      val columnName = "aColumn"
      val testDF = Seq(1.0, 3.0, 5.0).toDF(columnName)

      val average: Double = testDF.averageOfAColumn(spark, columnName)

      average should be(3.0)
    }

    scenario("calculate the average of a column in a dataframe") {
      val columnName = "aColumn"
      val testDF = Seq(1.0, 3.0).toDF(columnName)


      val average: Double = testDF.averageOfAColumn(spark, columnName)

      average should be(2.0)
    }
  }
}
