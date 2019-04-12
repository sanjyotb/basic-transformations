package thoughtworks

import org.scalatest.{Matchers}
import Analyzer._

class AnalyzerTest extends FeatureSpecWithSpark with Matchers {
  import spark.implicits._

  scenario("count the number of rows in a dataframe") {
    val testDF = Seq("row1", "row2").toDF()

    val count: Long = testDF.countRows(spark)

    count should be(2)
  }
}
