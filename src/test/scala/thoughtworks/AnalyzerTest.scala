package thoughtworks

import org.scalatest.Matchers
import thoughtworks.Analyzer._

class AnalyzerTest extends FeatureSpecWithSpark with Matchers {

  import spark.implicits._

  feature("filter premium cut diamonds") {
    scenario("filter premium cut diamonds") {
      val columnName = "cut"
      val testDF = Seq("premium", "Good", "Premium").toDF(columnName)

      val totalPremiumCutDiamonds = testDF.totalPremiumCutDiamonds(spark)

      totalPremiumCutDiamonds should be(2)
    }
  }
}
