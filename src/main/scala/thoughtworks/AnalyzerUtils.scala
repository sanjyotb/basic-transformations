package thoughtworks

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object AnalyzerUtils {

  implicit class Dataframe(val dataframe: Dataset[Row]) {

    def countRows(spark: SparkSession): Long = {
      dataframe.count()
    }

    //TODO :: Read from seq to DataSet[Row] and then return
    def averageOfAColumn(spark: SparkSession, columnName: String): Double = {
      dataframe
        .select(avg(columnName))
        .first()
        .getDouble(0)
    }

    private def getDouble(value: Any): Double = {
      value.toString.toDouble
    }

    def minimumOfAColumn(spark: SparkSession, columnName: String): Double = {
      getDouble(
        dataframe
          .select(min(columnName))
          .first()
          .get(0)
      )
    }

    def maximumOfAColumn(spark: SparkSession, columnName: String): Double = {
      getDouble(
        dataframe
          .select(max(columnName))
          .first()
          .get(0)
      )
    }

    def filterAColumn(spark: SparkSession, filterCondition: Column): Dataset[Row] = {
      dataframe
        .filter(filterCondition)
    }

    def addAColumn(spark: SparkSession, columnName: String, columnValue: Column): Dataset[Row] = {
      dataframe.withColumn(columnName, columnValue)
    }

    def dropDuplicateRecords(spark: SparkSession): Dataset[Row] = {
      dataframe.dropDuplicates()
    }

    def dropAColumn(spark: SparkSession, columnName: String): Dataset[Row] = {
      dataframe.drop(columnName)
    }
  }

}
