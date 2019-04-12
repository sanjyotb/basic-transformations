package thoughtworks

import org.apache.spark.sql.{Dataset, Row, SparkSession}

object Analyzer {
  implicit class DiamondsDataframe(val dataframe: Dataset[Row]) {

    def countRows(spark: SparkSession): Long = {
      dataframe.count()
    }

    def averageOfAColumn(spark: SparkSession, columnName: String): Double = {
      import spark.implicits._
      import org.apache.spark.sql.functions.avg

      val dataset: Dataset[Double] = dataframe.select(avg(columnName)).as[Double]

      dataset.collect()(0)
    }
  }

}
