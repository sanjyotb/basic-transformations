package thoughtworks

import org.apache.spark.sql.{Dataset, Row, SparkSession}

object Analyzer {
  implicit class DiamondsDataframe(val dataframe: Dataset[Row]) {

    def countRows(spark: SparkSession): Long = {
      dataframe.count()
    }
  }

}
